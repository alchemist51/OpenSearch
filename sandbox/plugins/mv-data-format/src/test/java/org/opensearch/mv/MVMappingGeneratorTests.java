/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class MVMappingGeneratorTests extends OpenSearchTestCase {

    private final MVMappingGenerator generator = new MVMappingGenerator();

    // ── Mapping does not expose DataFusion names ──────────────────────────

    @SuppressWarnings("unchecked")
    public void testMappingDoesNotExposeDataFusionNames() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg(
            "RegionID",
            "AdvEngineID",
            "ResolutionWidth",
            "ResolutionWidth",
            "ResolutionWidth"
        );

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertNotNull(properties);

        for (String field : properties.keySet()) {
            assertFalse("field must not contain 'Int64(1)': " + field, field.contains("Int64(1)"));
            assertFalse("field must not contain 'mv_input.': " + field, field.contains("mv_input."));
            assertFalse("field must not contain '[count]': " + field, field.contains("[count]"));
            assertFalse("field must not contain '[sum]': " + field, field.contains("[sum]"));
            assertFalse("field must not contain '[value]': " + field, field.contains("[value]"));
        }
    }

    // ── All aggregate types produce correct OS types ──────────────────────

    @SuppressWarnings("unchecked")
    public void testAllAggregateTypesProduceCorrectOsTypes() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg(
            "RegionID",
            "AdvEngineID",
            "ResolutionWidth",
            "ResolutionWidth",
            "ResolutionWidth"
        );

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        // COUNT → long
        Map<String, Object> cntField = (Map<String, Object>) properties.get("cnt");
        assertNotNull("COUNT field must exist", cntField);
        assertEquals("long", cntField.get("type"));

        // SUM → long
        Map<String, Object> sumField = (Map<String, Object>) properties.get("sum_AdvEngineID");
        assertNotNull("SUM field must exist", sumField);
        assertEquals("long", sumField.get("type"));

        // MIN → long
        Map<String, Object> minField = (Map<String, Object>) properties.get("min_ResolutionWidth");
        assertNotNull("MIN field must exist", minField);
        assertEquals("long", minField.get("type"));

        // MAX → long
        Map<String, Object> maxField = (Map<String, Object>) properties.get("max_ResolutionWidth");
        assertNotNull("MAX field must exist", maxField);
        assertEquals("long", maxField.get("type"));

        // AVG → double
        Map<String, Object> avgField = (Map<String, Object>) properties.get("avg_ResolutionWidth");
        assertNotNull("AVG field must exist", avgField);
        assertEquals("double", avgField.get("type"));
    }

    // ── Group keys appear correctly ───────────────────────────────────────

    @SuppressWarnings("unchecked")
    public void testGroupKeysAppearCorrectlyInMapping() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("service", GroupKey.ColumnType.KEYWORD), GroupKey.of("status", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("latency_ms", "lat_sum"))
        );

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        // Group keys
        Map<String, Object> serviceField = (Map<String, Object>) properties.get("service");
        assertNotNull("group key 'service' must exist", serviceField);
        assertEquals("keyword", serviceField.get("type"));

        Map<String, Object> statusField = (Map<String, Object>) properties.get("status");
        assertNotNull("group key 'status' must exist", statusField);
        assertEquals("keyword", statusField.get("type"));

        // Aggregates
        Map<String, Object> cntField = (Map<String, Object>) properties.get("cnt");
        assertNotNull("COUNT field must exist", cntField);
        assertEquals("long", cntField.get("type"));

        Map<String, Object> sumField = (Map<String, Object>) properties.get("lat_sum");
        assertNotNull("SUM field must exist", sumField);
        assertEquals("long", sumField.get("type"));
    }

    @SuppressWarnings("unchecked")
    public void testSingleLongGroupKey() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("UserID", "AdvEngineID", null, null, null);

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        Map<String, Object> userIdField = (Map<String, Object>) properties.get("UserID");
        assertNotNull("group key 'UserID' must exist", userIdField);
        assertEquals("long", userIdField.get("type"));
    }

    // ── Compatibility check ───────────────────────────────────────────────

    public void testIsCompatibleReturnsTrueForMatchingMapping() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        Map<String, Object> mapping = generator.generateMapping(def);

        assertTrue(generator.isCompatible(def, mapping));
    }

    @SuppressWarnings("unchecked")
    public void testIsCompatibleReturnsFalseForMissingField() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> mapping = Map.of("properties", Map.of("RegionID", Map.of("type", "long"))
        // Missing cnt and sum_AdvEngineID
        );

        assertFalse(generator.isCompatible(def, mapping));
    }

    public void testIsCompatibleReturnsFalseForWrongType() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> mapping = Map.of(
            "properties",
            Map.of(
                "RegionID",
                Map.of("type", "keyword"),  // wrong type
                "cnt",
                Map.of("type", "long"),
                "sum_AdvEngineID",
                Map.of("type", "long")
            )
        );

        assertFalse(generator.isCompatible(def, mapping));
    }

    public void testIsCompatibleReturnsFalseForEmptyMapping() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        assertFalse(generator.isCompatible(def, Map.of()));
    }

    // ── Mapping field count ───────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    public void testMappingFieldCountMatchesDefinition() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "Res", "Res", "Res");

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        // 1 group key + 5 agg aliases = 6
        assertEquals(6, properties.size());
    }

    @SuppressWarnings("unchecked")
    public void testAvgOnlyProducesUserAliasInMapping() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", null, null, null, "Latency");

        Map<String, Object> mapping = generator.generateMapping(def);
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        // Only RegionID, cnt, avg_Latency (not avg_count_Latency or avg_sum_Latency)
        assertEquals(3, properties.size());
        assertTrue(properties.containsKey("RegionID"));
        assertTrue(properties.containsKey("cnt"));
        assertTrue(properties.containsKey("avg_Latency"));
        // State columns should NOT appear in the user-facing mapping
        assertFalse(properties.containsKey("avg_count_Latency"));
        assertFalse(properties.containsKey("avg_sum_Latency"));
    }
}
