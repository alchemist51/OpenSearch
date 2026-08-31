/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MVCompiledDefinitionTests extends OpenSearchTestCase {

    // ── COUNT/SUM/MIN/MAX/AVG programmatic construction ───────────────────

    public void testForCountSumMinMaxAvgCreatesAllAggregates() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg(
            "RegionID",
            "AdvEngineID",
            "ResolutionWidth",
            "ResolutionWidth",
            "ResolutionWidth"
        );

        assertEquals(1, def.groupKeys().size());
        assertEquals("RegionID", def.groupKeys().get(0).name());
        assertEquals(GroupKey.ColumnType.LONG, def.groupKeys().get(0).columnType());

        // COUNT + SUM + MIN + MAX + AVG = 5 aggregates
        assertEquals(5, def.aggregates().size());
        assertEquals(AggregateSpec.AggFunction.COUNT, def.aggregates().get(0).function());
        assertEquals(AggregateSpec.AggFunction.SUM, def.aggregates().get(1).function());
        assertEquals(AggregateSpec.AggFunction.MIN, def.aggregates().get(2).function());
        assertEquals(AggregateSpec.AggFunction.MAX, def.aggregates().get(3).function());
        assertEquals(AggregateSpec.AggFunction.AVG, def.aggregates().get(4).function());
    }

    public void testNullFieldsOmitAggregates() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", null, null, null, null);

        // Only COUNT (always present)
        assertEquals(1, def.aggregates().size());
        assertEquals(AggregateSpec.AggFunction.COUNT, def.aggregates().get(0).function());
        assertEquals("cnt", def.aggregates().get(0).userAlias());
    }

    public void testGroupFieldRequired() {
        expectThrows(NullPointerException.class, () -> MVCompiledDefinition.forCountSumMinMaxAvg(null, "x", null, null, null));
    }

    // ── AVG decomposition ─────────────────────────────────────────────────

    public void testAvgDecomposesIntoCountAndSumState() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("GroupCol", null, null, null, "MetricCol");

        AggregateSpec avgSpec = def.aggregates()
            .stream()
            .filter(a -> a.function() == AggregateSpec.AggFunction.AVG)
            .findFirst()
            .orElseThrow();

        assertEquals("avg_MetricCol", avgSpec.userAlias());
        assertEquals("double", avgSpec.targetMappingType());

        // AVG decomposes into count_state + sum_state
        assertEquals(2, avgSpec.stateColumns().size());
        assertEquals("avg_count_MetricCol", avgSpec.stateColumns().get(0).name());
        assertEquals("long", avgSpec.stateColumns().get(0).physicalType());
        assertEquals("avg_sum_MetricCol", avgSpec.stateColumns().get(1).name());
        assertEquals("long", avgSpec.stateColumns().get(1).physicalType());
    }

    public void testAvgPartialSqlDecomposesIntoCountAndSum() {
        AggregateSpec avg = AggregateSpec.avg("Price");
        assertEquals("COUNT(\"Price\"), SUM(\"Price\")", avg.partialSqlFragment());
    }

    public void testAvgFoldSqlSumsCountAndSumStates() {
        AggregateSpec avg = AggregateSpec.avg("Price");
        assertEquals("SUM(\"avg_count_Price\"), SUM(\"avg_sum_Price\")", avg.foldSqlFragment());
    }

    // ── Stable aliases don't contain DataFusion names ─────────────────────

    public void testAliasesDoNotContainDataFusionInternalNames() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg(
            "RegionID",
            "AdvEngineID",
            "ResolutionWidth",
            "ResolutionWidth",
            "ResolutionWidth"
        );

        for (AggregateSpec agg : def.aggregates()) {
            String alias = agg.userAlias();
            assertFalse("alias must not contain DataFusion internals: " + alias, alias.contains("Int64(1)"));
            assertFalse("alias must not contain DataFusion internals: " + alias, alias.contains("mv_input."));
            assertFalse("alias must not contain DataFusion internals: " + alias, alias.contains("[count]"));
            assertFalse("alias must not contain DataFusion internals: " + alias, alias.contains("[sum]"));
            assertFalse("alias must not contain DataFusion internals: " + alias, alias.contains("[value]"));

            for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                assertFalse("state col must not contain DataFusion internals: " + sc.name(), sc.name().contains("Int64(1)"));
                assertFalse("state col must not contain DataFusion internals: " + sc.name(), sc.name().contains("mv_input."));
                assertFalse("state col must not contain DataFusion internals: " + sc.name(), sc.name().contains("[count]"));
                assertFalse("state col must not contain DataFusion internals: " + sc.name(), sc.name().contains("[sum]"));
            }
        }
    }

    public void testProjectionOrderDoesNotContainDataFusionNames() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, "ResolutionWidth");

        for (String col : def.projectionOrder()) {
            assertFalse("projection column must not contain DataFusion internals: " + col, col.contains("Int64(1)"));
            assertFalse("projection column must not contain DataFusion internals: " + col, col.contains("mv_input."));
        }
    }

    // ── Hash stability ────────────────────────────────────────────────────

    public void testHashStabilitySameInputSameHash() {
        MVCompiledDefinition def1 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        assertEquals(def1.hash(), def2.hash());
        assertNotNull(def1.hash());
        assertFalse(def1.hash().isEmpty());
    }

    public void testHashChangesWhenDefinitionChanges() {
        MVCompiledDefinition def1 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "OtherField", null, null, null);

        assertNotEquals(def1.hash(), def2.hash());
    }

    public void testHashChangesWhenGroupKeyChanges() {
        MVCompiledDefinition def1 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("UserID", "AdvEngineID", null, null, null);

        assertNotEquals(def1.hash(), def2.hash());
    }

    public void testHashChangesWhenAggregateAdded() {
        MVCompiledDefinition def1 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "AdvEngineID", null, null);

        assertNotEquals(def1.hash(), def2.hash());
    }

    public void testHashIsSha256Hex() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        String hash = def.hash();
        // SHA-256 hex = 64 chars
        assertEquals(64, hash.length());
        assertTrue(hash.matches("[0-9a-f]{64}"));
    }

    // ── Partial SQL generation ────────────────────────────────────────────

    public void testBuildPartialSqlCountSum() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        String sql = def.buildPartialSql("mv_input");

        assertEquals("SELECT \"RegionID\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY \"RegionID\"", sql);
    }

    public void testBuildPartialSqlAllAggregates() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "Res", "Res", "Res");

        String sql = def.buildPartialSql("mv_input");

        // AVG decomposes into COUNT + SUM in partial
        assertEquals(
            "SELECT \"RegionID\", COUNT(*), SUM(\"AdvEngineID\"), MIN(\"Res\"), MAX(\"Res\"), COUNT(\"Res\"), SUM(\"Res\")"
                + " FROM mv_input GROUP BY \"RegionID\"",
            sql
        );
    }

    public void testBuildFoldSqlCountSum() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        String sql = def.buildFoldSql("__MV_STATES__");

        assertEquals("SELECT \"RegionID\", SUM(\"cnt\"), SUM(\"sum_AdvEngineID\") FROM __MV_STATES__ GROUP BY \"RegionID\"", sql);
    }

    public void testBuildFoldSqlWithAvg() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", null, null, null, "Price");

        String sql = def.buildFoldSql("state_table");

        assertEquals(
            "SELECT \"RegionID\", SUM(\"cnt\"), SUM(\"avg_count_Price\"), SUM(\"avg_sum_Price\") FROM state_table GROUP BY \"RegionID\"",
            sql
        );
    }

    // ── Projection order ──────────────────────────────────────────────────

    public void testProjectionOrderGroupKeysFirst() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, "Res");

        List<String> projection = def.projectionOrder();

        // RegionID, cnt, sum_AdvEngineID, avg_count_Res, avg_sum_Res
        assertEquals(5, projection.size());
        assertEquals("RegionID", projection.get(0));
        assertEquals("cnt", projection.get(1));
        assertEquals("sum_AdvEngineID", projection.get(2));
        assertEquals("avg_count_Res", projection.get(3));
        assertEquals("avg_sum_Res", projection.get(4));
    }

    public void testProjectionOrderMultipleGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("service", GroupKey.ColumnType.KEYWORD), GroupKey.of("status", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("latency_ms", "lat_sum"))
        );

        List<String> projection = def.projectionOrder();
        assertEquals(List.of("service", "status", "cnt", "lat_sum"), projection);
    }

    // ── Schema validation ─────────────────────────────────────────────────

    public void testValidateSchemaPassesWithCorrectSchema() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> schema = new HashMap<>();
        schema.put("RegionID", "long");
        schema.put("cnt", "long");
        schema.put("sum_AdvEngineID", "long");

        // Should not throw
        def.validateSchema(schema);
    }

    public void testValidateSchemaFailsOnMissingField() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> schema = new HashMap<>();
        schema.put("RegionID", "long");
        // Missing cnt and sum_AdvEngineID

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> def.validateSchema(schema));
        assertTrue(ex.getMessage().contains("missing field"));
        assertTrue(ex.getMessage().contains(def.hash()));
    }

    // ── Generic builder ───────────────────────────────────────────────────

    public void testOfRequiresGroupKeys() {
        expectThrows(IllegalArgumentException.class, () -> MVCompiledDefinition.of(List.of(), List.of(AggregateSpec.count("cnt"))));
    }

    public void testOfRequiresAggregates() {
        expectThrows(
            IllegalArgumentException.class,
            () -> MVCompiledDefinition.of(List.of(GroupKey.of("k", GroupKey.ColumnType.LONG)), List.of())
        );
    }

    // ── Target mapping ────────────────────────────────────────────────────

    public void testTargetMappingContainsAllFields() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, "Res");

        Map<String, String> mapping = def.targetMapping();

        assertEquals("long", mapping.get("RegionID"));
        assertEquals("long", mapping.get("cnt"));
        assertEquals("long", mapping.get("sum_AdvEngineID"));
        assertEquals("double", mapping.get("avg_Res"));
        assertEquals(4, mapping.size());
    }

    public void testTargetMappingDoesNotExposeDataFusionNames() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "Res", "Res", "Res");

        for (String field : def.targetMapping().keySet()) {
            assertFalse("mapping field must not contain DataFusion internals: " + field, field.contains("Int64(1)"));
            assertFalse("mapping field must not contain DataFusion internals: " + field, field.contains("mv_input."));
            assertFalse("mapping field must not contain DataFusion internals: " + field, field.contains("[count]"));
            assertFalse("mapping field must not contain DataFusion internals: " + field, field.contains("[sum]"));
        }
    }
}
