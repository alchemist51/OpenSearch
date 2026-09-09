/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for the MV definition layer:
 * (a) definition descriptor JSON round-trip
 * (b) dual-write atomicity via cluster state
 * (c) MVGroupByOrdering sort derivation
 * (d) MVMappingGenerator auto-inherit
 */
public class MVDefinitionLayerTests extends OpenSearchTestCase {

    // ── (a) Definition descriptor JSON round-trip ────────────────────────

    public void testDescriptorRoundTrip() throws IOException {
        // Build a definition with mixed key types and aggregates
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("service", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("status", GroupKey.ColumnType.LONG)
            ),
            List.of(
                AggregateSpec.count("cnt"),
                AggregateSpec.sum("latency_ms", "lat_sum"),
                AggregateSpec.min("latency_ms", "lat_min"),
                AggregateSpec.max("latency_ms", "lat_max")
            )
        );

        // Export to descriptor
        MVDefinitionDescriptor descriptor = def.toDescriptor();

        // Serialize to JSON
        String json;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
            json = BytesReference.bytes(builder).utf8ToString();
        }

        // Parse back
        MVDefinitionDescriptor parsed;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, json
        )) {
            parsed = MVDefinitionDescriptor.fromXContent(parser);
        }

        // Rebuild compiled definition
        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(parsed);

        // The hashes must match — exact same definition
        assertEquals(def.hash(), rebuilt.hash());
        assertEquals(def.groupKeys().size(), rebuilt.groupKeys().size());
        assertEquals(def.aggregates().size(), rebuilt.aggregates().size());
        assertEquals(def.stateColumnNames(), rebuilt.stateColumnNames());
    }

    public void testDescriptorIntegrityHashMismatchFails() throws IOException {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("x", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVDefinitionDescriptor descriptor = def.toDescriptor();

        // Serialize
        String json;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
            json = BytesReference.bytes(builder).utf8ToString();
        }

        // Tamper: change the hash
        String tampered = json.replace(def.hash(), "0000000000000000000000000000000000000000000000000000000000000000");

        MVDefinitionDescriptor parsedTampered;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, tampered
        )) {
            parsedTampered = MVDefinitionDescriptor.fromXContent(parser);
        }

        // fromDescriptor should reject the tampered hash
        expectThrows(IllegalArgumentException.class, () -> MVCompiledDefinition.fromDescriptor(parsedTampered));
    }

    // ── (b) Dual-write atomicity via cluster state ──────────────────────

    public void testDualWriteAtomicityBothCustomsCorrect() {
        // Simulate what MVDefinitionClusterService.execute() does
        String sourceIndex = "test_source";
        String targetIndex = "test_source_mv_test_view";
        String mvId = "test_view";

        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("region", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("amount", "sum_amount"))
        );
        String descriptorJson = serializeDescriptor(def.toDescriptor());

        // Build initial cluster state with source and target
        IndexMetadata sourceMeta = IndexMetadata.builder(sourceIndex)
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .build();
        IndexMetadata targetMeta = IndexMetadata.builder(targetIndex)
            .settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .build();

        ClusterState initial = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().put(sourceMeta, false).put(targetMeta, false))
            .build();

        // Apply the dual-write (simulating the ClusterStateUpdateTask.execute)
        Metadata.Builder metadata = Metadata.builder(initial.metadata());

        // Source: put mv_definitions
        Map<String, String> defs = Map.of(mvId, descriptorJson);
        metadata.put(IndexMetadata.builder(sourceMeta).putCustom(MVConstants.SOURCE_DEFINITIONS_KEY, defs));

        // Target: put mv_binding
        Map<String, String> binding = Map.of("source", sourceIndex, "mv_id", mvId, "descriptor", descriptorJson);
        metadata.put(IndexMetadata.builder(targetMeta).putCustom(MVConstants.TARGET_BINDING_KEY, binding));

        ClusterState newState = ClusterState.builder(initial).metadata(metadata).build();

        // Verify BOTH customs are present
        IndexMetadata updatedSource = newState.metadata().index(sourceIndex);
        assertNotNull(updatedSource.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY));
        assertEquals(descriptorJson, updatedSource.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY).get(mvId));

        IndexMetadata updatedTarget = newState.metadata().index(targetIndex);
        assertNotNull(updatedTarget.getCustomData(MVConstants.TARGET_BINDING_KEY));
        assertEquals(sourceIndex, updatedTarget.getCustomData(MVConstants.TARGET_BINDING_KEY).get("source"));
        assertEquals(mvId, updatedTarget.getCustomData(MVConstants.TARGET_BINDING_KEY).get("mv_id"));
        assertEquals(descriptorJson, updatedTarget.getCustomData(MVConstants.TARGET_BINDING_KEY).get("descriptor"));
    }

    public void testFailedValidationMutatesNothing() {
        // An invalid definition (empty group keys) should throw before any state mutation
        expectThrows(IllegalArgumentException.class, () ->
            MVCompiledDefinition.of(List.of(), List.of(AggregateSpec.count("cnt")))
        );

        // An invalid definition (empty aggregates)
        expectThrows(IllegalArgumentException.class, () ->
            MVCompiledDefinition.of(List.of(GroupKey.of("x", GroupKey.ColumnType.LONG)), List.of())
        );
    }

    // ── (c) MVGroupByOrdering sort derivation ───────────────────────────

    public void testSortDerivationSingleKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("region", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        assertEquals(1, ordering.size());
        assertEquals(0, ordering.keys().get(0).stateFieldIndex());
        assertEquals("region", ordering.keys().get(0).column());
        assertEquals(MVGroupByOrdering.Direction.ASCENDING, ordering.keys().get(0).direction());
        assertEquals(MVGroupByOrdering.NullPlacement.NULLS_FIRST, ordering.keys().get(0).nullPlacement());
    }

    public void testSortDerivationMultiKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("service", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("status", GroupKey.ColumnType.LONG),
                GroupKey.of("region", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        assertEquals(3, ordering.size());
        // Verify order matches group key declaration order
        assertEquals("service", ordering.keys().get(0).column());
        assertEquals(0, ordering.keys().get(0).stateFieldIndex());
        assertEquals("status", ordering.keys().get(1).column());
        assertEquals(1, ordering.keys().get(1).stateFieldIndex());
        assertEquals("region", ordering.keys().get(2).column());
        assertEquals(2, ordering.keys().get(2).stateFieldIndex());

        // SQL ORDER BY should cover all keys
        String sql = ordering.toSqlOrderBy();
        assertTrue(sql.contains("\"service\""));
        assertTrue(sql.contains("\"status\""));
        assertTrue(sql.contains("\"region\""));
        assertTrue(sql.contains("ASC"));
        assertTrue(sql.contains("NULLS FIRST"));
    }

    public void testSortDerivationDerivedKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 300000L, "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        assertEquals(1, ordering.size());
        assertEquals("bucket", ordering.keys().get(0).column());
        // SQL expression should be the date_bin, not just the alias
        assertTrue(ordering.keys().get(0).sqlExpression().contains("date_bin"));
    }

    // ── (d) MVMappingGenerator auto-inherit ─────────────────────────────

    public void testMappingGeneratorBasic() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("service", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("count", GroupKey.ColumnType.LONG)
            ),
            List.of(
                AggregateSpec.count("cnt"),
                AggregateSpec.sum("latency", "lat_sum"),
                AggregateSpec.min("latency", "lat_min"),
                AggregateSpec.max("latency", "lat_max")
            )
        );

        MVMappingGenerator gen = new MVMappingGenerator();
        Map<String, Object> mapping = gen.generateMapping(def);

        @SuppressWarnings("unchecked")
        Map<String, Object> props = (Map<String, Object>) mapping.get("properties");
        assertNotNull(props);

        // Group keys
        assertFieldType(props, "service", "keyword");
        assertFieldType(props, "count", "long");

        // Aggregates
        assertFieldType(props, "cnt", "long");
        assertFieldType(props, "lat_sum", "long");
        assertFieldType(props, "lat_min", "long");
        assertFieldType(props, "lat_max", "long");
    }

    public void testMappingCompatibility() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("x", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );

        MVMappingGenerator gen = new MVMappingGenerator();
        Map<String, Object> mapping = gen.generateMapping(def);

        // Compatible
        assertTrue(gen.isCompatible(def, mapping));

        // Incompatible: wrong type
        @SuppressWarnings("unchecked")
        Map<String, Object> badProps = (Map<String, Object>) mapping.get("properties");
        @SuppressWarnings("unchecked")
        Map<String, Object> xField = (Map<String, Object>) badProps.get("x");
        xField.put("type", "keyword");  // was "long"
        assertFalse(gen.isCompatible(def, mapping));
    }

    public void testMappingGeneratorAvg() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("x", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.avg("latency"))
        );

        MVMappingGenerator gen = new MVMappingGenerator();
        Map<String, Object> mapping = gen.generateMapping(def);

        @SuppressWarnings("unchecked")
        Map<String, Object> props = (Map<String, Object>) mapping.get("properties");
        // AVG maps to double
        assertFieldType(props, "avg_latency", "double");
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    // ── (e) MVViewCreation target contract stamping ─────────────────────

    public void testBuildTargetSettingsStampsIndexSort() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("service", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("region", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("latency", "lat_sum"))
        );
        String descriptorJson = serializeDescriptor(MVDefinitionDescriptor.fromCompiled(def));
        Settings settings = MVViewCreation.buildTargetSettings("source_idx", 2, def, descriptorJson);

        // index.sort.field must carry the ordered group keys
        List<String> sortFields = settings.getAsList("index.sort.field");
        assertEquals(List.of("service", "region"), sortFields);

        // index.sort.order must be ASC for every key
        List<String> sortOrders = settings.getAsList("index.sort.order");
        assertEquals(List.of("asc", "asc"), sortOrders);

        // index.sort.missing must be _first for every key
        List<String> sortMissing = settings.getAsList("index.sort.missing");
        assertEquals(List.of("_first", "_first"), sortMissing);
    }

    public void testBuildTargetSettingsStampsStateFields() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("region", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("val", "val_sum"))
        );
        String descriptorJson = serializeDescriptor(MVDefinitionDescriptor.fromCompiled(def));
        Settings settings = MVViewCreation.buildTargetSettings("src", 1, def, descriptorJson);

        List<String> stateFields = settings.getAsList(MVConstants.STATE_FIELDS_SETTING);
        assertFalse("state_fields must not be empty", stateFields.isEmpty());
        // Group keys come first, then aggregates
        assertEquals("region", stateFields.get(0));
        assertTrue("state_fields must contain cnt", stateFields.contains("cnt"));
        assertTrue("state_fields must contain val_sum", stateFields.contains("val_sum"));
    }

    public void testBuildTargetSettingsStampsDerivedDataFormat() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("c"))
        );
        String descriptorJson = serializeDescriptor(MVDefinitionDescriptor.fromCompiled(def));
        Settings settings = MVViewCreation.buildTargetSettings("src", 1, def, descriptorJson);

        assertEquals(MVConstants.DATA_FORMAT_NAME, settings.get(MVConstants.DERIVED_DATA_FORMAT_SETTING));
        assertEquals("true", settings.get(MVConstants.DERIVED_INDEX_SETTING));
        assertEquals("true", settings.get(MVConstants.STATE_MERGE_SETTING));
    }

    public void testBuildTargetSettingsSingleKeySortMatchesOrdering() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 300000L, "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        String descriptorJson = serializeDescriptor(MVDefinitionDescriptor.fromCompiled(def));
        Settings settings = MVViewCreation.buildTargetSettings("src", 1, def, descriptorJson);

        // Sort field must match the ordering's column name, not the SQL expression
        List<String> sortFields = settings.getAsList("index.sort.field");
        assertEquals(1, sortFields.size());
        assertEquals("bucket", sortFields.get(0));
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private void assertFieldType(Map<String, Object> props, String field, String expectedType) {
        Object fieldObj = props.get(field);
        assertNotNull("field [" + field + "] not found in mapping", fieldObj);
        assertTrue(fieldObj instanceof Map);
        assertEquals(expectedType, ((Map<String, Object>) fieldObj).get("type"));
    }

    private static String serializeDescriptor(MVDefinitionDescriptor descriptor) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
