/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.engine.dataformat.MVWriterConfigRegistry;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests that MV definitions stored in IndexMetadata customData are correctly
 * compiled into FFI-ready specs for the Rust partial builder.
 */
public class MVWriterConfigTests extends OpenSearchTestCase {

    /**
     * Build a customData map with one MV definition serialized as XContent.
     */
    private Map<String, String> buildCustomDataWithOneMV() throws IOException {
        // Create a descriptor
        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(
                MVDefinitionDescriptor.GroupKeyDescriptor.plain("region", GroupKey.ColumnType.KEYWORD),
                MVDefinitionDescriptor.GroupKeyDescriptor.span("event_bucket", 300000, "EventTime")
            ),
            List.of(
                MVDefinitionDescriptor.AggregateDescriptor.count("cnt"),
                MVDefinitionDescriptor.AggregateDescriptor.sum("HitCount", "total_hits"),
                MVDefinitionDescriptor.AggregateDescriptor.min("ResponseTime", "min_rt"),
                MVDefinitionDescriptor.AggregateDescriptor.max("ResponseTime", "max_rt")
            )
        );

        // IndexMetadata.getCustomData("mv_definitions") returns the flat map
        // directly: mvId -> descriptor JSON.
        XContentBuilder builder = XContentFactory.jsonBuilder();
        descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
        return Map.of("mv1", builder.toString());
    }

    public void testFromCustomDataProducesCorrectSpecs() throws IOException {
        Map<String, String> customData = buildCustomDataWithOneMV();
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(customData);

        assertEquals("Expected 1 MV spec", 1, specs.size());
        MVWriterConfig.MVPartialWriterSpec spec = specs.get(0);
        assertEquals("mv1", spec.mvId());
        assertNotNull(spec.definitionHash());
        assertFalse(spec.definitionHash().isEmpty());

        // Group keys: region (keyword/utf8) and event_bucket (timestamp_ms)
        assertEquals(List.of("region", "event_bucket"), spec.groupColNames());
        assertEquals(List.of("utf8", "timestamp_ms"), spec.groupColTypes());

        // Span key derivation metadata: plain keys point at themselves with span 0,
        // the span key points at its date source with the bucket width in ms.
        assertEquals(List.of("region", "EventTime"), spec.groupColSources());
        assertEquals(List.of(0L, 300000L), spec.groupSpanMs());

        // ...and it survives the bridge into the server registry record.
        List<MVWriterConfigRegistry.MVPartialWriterSpec> registrySpecs = MVWriterConfig.fromCustomDataToRegistrySpecs(customData);
        assertEquals(1, registrySpecs.size());
        assertEquals(List.of("region", "EventTime"), registrySpecs.get(0).groupColSources());
        assertEquals(List.of(0L, 300000L), registrySpecs.get(0).groupSpanMs());

        // Sort keys = all group keys
        assertEquals(List.of("region", "event_bucket"), spec.sortKeyNames());

        // 4 aggregates: count(*), sum, min, max
        assertEquals(4, spec.aggSpecs().size());

        MVWriterConfig.MVPartialWriterSpec.AggFFI cnt = spec.aggSpecs().get(0);
        assertEquals("count", cnt.function());
        assertNull(cnt.sourceField());
        assertEquals(List.of("cnt"), cnt.outputNames());

        MVWriterConfig.MVPartialWriterSpec.AggFFI sum = spec.aggSpecs().get(1);
        assertEquals("sum", sum.function());
        assertEquals("HitCount", sum.sourceField());
        assertEquals(List.of("total_hits"), sum.outputNames());

        MVWriterConfig.MVPartialWriterSpec.AggFFI min = spec.aggSpecs().get(2);
        assertEquals("min", min.function());
        assertEquals("ResponseTime", min.sourceField());
        assertEquals(List.of("min_rt"), min.outputNames());

        MVWriterConfig.MVPartialWriterSpec.AggFFI max = spec.aggSpecs().get(3);
        assertEquals("max", max.function());
        assertEquals("ResponseTime", max.sourceField());
        assertEquals(List.of("max_rt"), max.outputNames());
    }

    public void testFromCustomDataWithNullReturnsEmpty() {
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(null);
        assertTrue(specs.isEmpty());
    }

    public void testFromCustomDataWithEmptyReturnsEmpty() {
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(Map.of());
        assertTrue(specs.isEmpty());
    }

    public void testFromCustomDataWithBlankValueReturnsEmpty() {
        Map<String, String> customData = Map.of("mv1", "  ");
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(customData);
        assertTrue(specs.isEmpty());
    }

    public void testFromCustomDataWithBadJsonDoesNotThrow() {
        Map<String, String> customData = Map.of("mv1", "{invalid json!!!");
        // Should not throw — errors are logged and an empty list returned
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(customData);
        assertTrue(specs.isEmpty());
    }

    public void testCompileToFFISpecCountField() {
        // COUNT(field) should be serialized as "count_field" function
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("status", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.countField("user_id", "user_cnt"))
        );
        MVWriterConfig.MVPartialWriterSpec spec = MVWriterConfig.compileToFFISpec("mv_count", def);
        assertEquals("count_field", spec.aggSpecs().get(0).function());
        assertEquals("user_id", spec.aggSpecs().get(0).sourceField());
    }

    public void testCompileToFFISpecAvg() {
        // AVG decomposes into two state columns
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("status", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.avg("latency"))
        );
        MVWriterConfig.MVPartialWriterSpec spec = MVWriterConfig.compileToFFISpec("mv_avg", def);
        // AVG function name — the Rust side treats AVG as COUNT + SUM decomposition
        // The Java AggFunction for AVG is still AVG in the enum
        assertEquals("avg", spec.aggSpecs().get(0).function());
        assertEquals("latency", spec.aggSpecs().get(0).sourceField());
        // Two output columns: avg_count_latency, avg_sum_latency
        assertEquals(List.of("avg_count_latency", "avg_sum_latency"), spec.aggSpecs().get(0).outputNames());
    }

    public void testMultipleMVDefinitions() throws IOException {
        MVDefinitionDescriptor desc1 = MVDefinitionDescriptor.of(
            List.of(MVDefinitionDescriptor.GroupKeyDescriptor.plain("region", GroupKey.ColumnType.KEYWORD)),
            List.of(MVDefinitionDescriptor.AggregateDescriptor.count("cnt"))
        );
        MVDefinitionDescriptor desc2 = MVDefinitionDescriptor.of(
            List.of(MVDefinitionDescriptor.GroupKeyDescriptor.plain("status", GroupKey.ColumnType.INTEGER)),
            List.of(MVDefinitionDescriptor.AggregateDescriptor.sum("bytes", "total_bytes"))
        );

        XContentBuilder builder1 = XContentFactory.jsonBuilder();
        desc1.toXContent(builder1, ToXContent.EMPTY_PARAMS);
        XContentBuilder builder2 = XContentFactory.jsonBuilder();
        desc2.toXContent(builder2, ToXContent.EMPTY_PARAMS);

        Map<String, String> customData = Map.of(
            "mv_region", builder1.toString(),
            "mv_status", builder2.toString()
        );
        List<MVWriterConfig.MVPartialWriterSpec> specs = MVWriterConfig.fromCustomData(customData);

        assertEquals(2, specs.size());
        // Order may vary — find by mvId
        MVWriterConfig.MVPartialWriterSpec regionSpec = specs.stream()
            .filter(s -> s.mvId().equals("mv_region")).findFirst().orElseThrow();
        MVWriterConfig.MVPartialWriterSpec statusSpec = specs.stream()
            .filter(s -> s.mvId().equals("mv_status")).findFirst().orElseThrow();

        assertEquals(List.of("region"), regionSpec.groupColNames());
        assertEquals(List.of("utf8"), regionSpec.groupColTypes());
        assertEquals(List.of("status"), statusSpec.groupColNames());
        assertEquals(List.of("int32"), statusSpec.groupColTypes());
    }
}
