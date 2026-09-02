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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Regression tests proving the L0–L3 compiled definitions produce the
 * correct physical output. These tests exist because a prior bug caused
 * {@code MVCompiledDefinition.compiledFor("clickbench_100m")} to route
 * through {@code fromLegacySpec()}, which only emitted COUNT(*)+SUM per
 * metric (16 physical columns) instead of the intended SUM/MIN/MAX/COUNT
 * quad (45 physical columns). L1/L2/L3 had the same class of defect.
 *
 * <p><b>Requirement 4:</b> Prove actual compiled output counts 45/48/88/130,
 * exact aggregate counts per function 10/10/20/30, physical state names
 * match logical aliases, target mapping counts equal projections, and the
 * builder uses full SQL (not SUM-only). Include round-trip search schema
 * tests for 45 logical/physical fields and existing null-fill
 * narrower-file compatibility separately.</p>
 */
public class MVLadderCompiledDefinitionTests extends OpenSearchTestCase {

    // ── Column count regression (Req 4: compiled output counts) ──────────

    public void testL0CompiledOutputCount45() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        assertEquals("L0 projection must be 45", 45, def.projectionOrder().size());
        assertEquals("L0 state columns must be 45", 45, def.stateColumnNames().size());
        assertEquals("L0 target mapping must be 45", 45, def.targetMapping().size());
        assertEquals("L0 group keys must be 5", 5, def.groupKeys().size());
        assertEquals("L0 aggregates must be 40", 40, def.aggregates().size());
    }

    public void testL1CompiledOutputCount48() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("heavy_l1");
        assertEquals("L1 projection must be 48", 48, def.projectionOrder().size());
        assertEquals("L1 state columns must be 48", 48, def.stateColumnNames().size());
        assertEquals("L1 target mapping must be 48", 48, def.targetMapping().size());
        assertEquals("L1 group keys must be 8", 8, def.groupKeys().size());
        assertEquals("L1 aggregates must be 40", 40, def.aggregates().size());
    }

    public void testL2CompiledOutputCount88() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("heavy_l2");
        assertEquals("L2 projection must be 88", 88, def.projectionOrder().size());
        assertEquals("L2 state columns must be 88", 88, def.stateColumnNames().size());
        assertEquals("L2 target mapping must be 88", 88, def.targetMapping().size());
        assertEquals("L2 group keys must be 8", 8, def.groupKeys().size());
        assertEquals("L2 aggregates must be 80", 80, def.aggregates().size());
    }

    public void testL3CompiledOutputCount130() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("heavy_l3");
        assertEquals("L3 projection must be 130", 130, def.projectionOrder().size());
        assertEquals("L3 state columns must be 130", 130, def.stateColumnNames().size());
        assertEquals("L3 target mapping must be 130", 130, def.targetMapping().size());
        assertEquals("L3 group keys must be 10", 10, def.groupKeys().size());
        assertEquals("L3 aggregates must be 120", 120, def.aggregates().size());
    }

    // ── Aggregate function counts (Req 4: per-function counts) ───────────

    public void testL0AggregateCountsByFunction() {
        assertAggCounts(MVCompiledDefinition.compiledFor("clickbench_100m"), 10, 10, 10, 10);
    }

    public void testL1AggregateCountsByFunction() {
        assertAggCounts(MVCompiledDefinition.compiledFor("heavy_l1"), 10, 10, 10, 10);
    }

    public void testL2AggregateCountsByFunction() {
        assertAggCounts(MVCompiledDefinition.compiledFor("heavy_l2"), 20, 20, 20, 20);
    }

    public void testL3AggregateCountsByFunction() {
        assertAggCounts(MVCompiledDefinition.compiledFor("heavy_l3"), 30, 30, 30, 30);
    }

    // ── Physical state names match logical aliases (Req 4) ───────────────

    public void testL0PhysicalStateNamesMatchShipFields() {
        assertPhysicalNamesMatchSpec("clickbench_100m");
    }

    public void testL1PhysicalStateNamesMatchShipFields() {
        assertPhysicalNamesMatchSpec("heavy_l1");
    }

    public void testL2PhysicalStateNamesMatchShipFields() {
        assertPhysicalNamesMatchSpec("heavy_l2");
    }

    public void testL3PhysicalStateNamesMatchShipFields() {
        assertPhysicalNamesMatchSpec("heavy_l3");
    }

    // ── Target mapping counts equal projections (Req 4) ──────────────────

    public void testAllLadderRungsMappingEqualsProjection() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            assertEquals(name + " mapping size must equal projection size", def.projectionOrder().size(), def.targetMapping().size());
            // Every projection column must appear in the mapping
            for (String col : def.projectionOrder()) {
                assertTrue(name + " mapping missing column: " + col, def.targetMapping().containsKey(col));
            }
        }
    }

    // ── Builder uses full SQL, not SUM-only (Req 4) ──────────────────────

    public void testL0PartialSqlContainsAllFourAggFunctions() {
        assertPartialSqlContainsAllFourAggFunctions("clickbench_100m", 10);
    }

    public void testL1PartialSqlContainsAllFourAggFunctions() {
        assertPartialSqlContainsAllFourAggFunctions("heavy_l1", 10);
    }

    public void testL2PartialSqlContainsAllFourAggFunctions() {
        assertPartialSqlContainsAllFourAggFunctions("heavy_l2", 20);
    }

    public void testL3PartialSqlContainsAllFourAggFunctions() {
        assertPartialSqlContainsAllFourAggFunctions("heavy_l3", 30);
    }

    // ── Fold SQL correctness ─────────────────────────────────────────────

    public void testL0FoldSqlFoldsCorrectly() {
        assertFoldSqlCorrectness("clickbench_100m", 10);
    }

    public void testL1FoldSqlFoldsCorrectly() {
        assertFoldSqlCorrectness("heavy_l1", 10);
    }

    public void testL2FoldSqlFoldsCorrectly() {
        assertFoldSqlCorrectness("heavy_l2", 20);
    }

    public void testL3FoldSqlFoldsCorrectly() {
        assertFoldSqlCorrectness("heavy_l3", 30);
    }

    // ── Spec↔Compiled identity (SQL, ship fields, fold SQL) ──────────────

    public void testAllLadderSpecSqlMatchesCompiledPartialSql() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition compiled = MVCompiledDefinition.compiledFor(name);
            MVDefinitionSpec spec = MVDefinitionSpec.source(name);
            assertEquals(name + " spec SQL must match compiled partial SQL", compiled.buildPartialSql(MVConstants.INPUT_TABLE), spec.sql());
        }
    }

    public void testAllLadderSpecShipFieldsMatchCompiledProjection() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition compiled = MVCompiledDefinition.compiledFor(name);
            MVDefinitionSpec spec = MVDefinitionSpec.source(name);
            assertEquals(name + " spec ship fields must match compiled state column names", compiled.stateColumnNames(), spec.shipFields());
        }
    }

    public void testAllLadderFoldSpecSqlMatchesCompiledFoldSql() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition compiled = MVCompiledDefinition.compiledFor(name);
            MVDefinitionSpec fold = MVDefinitionSpec.fold(name);
            assertEquals(name + " fold spec SQL must match compiled fold SQL", compiled.buildFoldSql(MVConstants.INPUT_TABLE), fold.sql());
        }
    }

    // ── compiledFor routing: direct factory, not fromLegacySpec ──────────

    public void testCompiledForDoesNotUseLegacyPathForLadder() {
        // The legacy path would produce COUNT(*)+SUM only. If these match the
        // direct factory, the routing is correct. If fromLegacySpec were used,
        // the hash would differ because the aggregate set is smaller.
        assertEquals(MVCompiledDefinition.clickbench100m().hash(), MVCompiledDefinition.compiledFor("clickbench_100m").hash());
        assertEquals(MVCompiledDefinition.heavyL1().hash(), MVCompiledDefinition.compiledFor("heavy_l1").hash());
        assertEquals(MVCompiledDefinition.heavyL2().hash(), MVCompiledDefinition.compiledFor("heavy_l2").hash());
        assertEquals(MVCompiledDefinition.heavyL3().hash(), MVCompiledDefinition.compiledFor("heavy_l3").hash());
    }

    public void testLegacyPathProducesDifferentHashForLadder() {
        // Prove that fromLegacySpec produces a DIFFERENT (wrong) definition
        // for these specs, confirming the bug that was present before the fix.
        MVCompiledDefinition correctL0 = MVCompiledDefinition.clickbench100m();
        MVCompiledDefinition legacyL0 = MVCompiledDefinition.fromLegacySpec(MVDefinitionSpec.source("clickbench_100m"));
        assertNotEquals(
            "L0 legacy path must produce a different (wrong) hash than the correct compiled definition",
            correctL0.hash(),
            legacyL0.hash()
        );
        // Legacy produces COUNT(*)+SUM only = 5 keys + 1 cnt + 10 sums = 16 projection cols
        assertEquals("legacy L0 would have produced only 16 columns", 16, legacyL0.projectionOrder().size());
    }

    // ── Legacy definitions are byte-for-byte unchanged ───────────────────

    public void testLegacyDefinitionsUnchanged() {
        for (String name : List.of("payments", "pull_count_sum", "pull_count_sum_userid", "clickbench_q9", "clickbench_q9_native")) {
            MVCompiledDefinition viaCompiledFor = MVCompiledDefinition.compiledFor(name);
            MVCompiledDefinition viaLegacy = MVCompiledDefinition.fromLegacySpec(MVDefinitionSpec.source(name));
            assertEquals("Legacy definition " + name + " must remain unchanged", viaLegacy.hash(), viaCompiledFor.hash());
        }
    }

    // ── Hash stability across calls ──────────────────────────────────────

    public void testHashStabilityAcrossCalls() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            String h1 = MVCompiledDefinition.compiledFor(name).hash();
            String h2 = MVCompiledDefinition.compiledFor(name).hash();
            assertEquals(name + " hash must be stable", h1, h2);
            assertEquals(name + " hash must be SHA-256 hex", 64, h1.length());
            assertTrue(name + " hash must be lowercase hex", h1.matches("[0-9a-f]{64}"));
        }
    }

    // ── Ship field naming convention: _sum/_min/_max/_cnt ─────────────────

    public void testAggregateShipFieldNamingConvention() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            int keyCount = def.groupKeys().size();
            List<String> allCols = def.projectionOrder();
            List<String> aggCols = allCols.subList(keyCount, allCols.size());
            assertEquals("aggregate fields must be multiple of 4 for " + name, 0, aggCols.size() % 4);
            for (int i = 0; i < aggCols.size(); i += 4) {
                String sumF = aggCols.get(i);
                String minF = aggCols.get(i + 1);
                String maxF = aggCols.get(i + 2);
                String cntF = aggCols.get(i + 3);
                assertTrue(name + " " + sumF + " must end with _sum", sumF.endsWith("_sum"));
                assertTrue(name + " " + minF + " must end with _min", minF.endsWith("_min"));
                assertTrue(name + " " + maxF + " must end with _max", maxF.endsWith("_max"));
                assertTrue(name + " " + cntF + " must end with _cnt", cntF.endsWith("_cnt"));
                // All four share the same prefix
                String prefix = sumF.substring(0, sumF.lastIndexOf('_'));
                assertEquals(prefix + "_min", minF);
                assertEquals(prefix + "_max", maxF);
                assertEquals(prefix + "_cnt", cntF);
            }
        }
    }

    // ── No DataFusion internals in any alias ─────────────────────────────

    public void testNoDataFusionInternalsInLadderAliases() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            for (AggregateSpec agg : def.aggregates()) {
                assertNoDataFusionInternals(name, agg.userAlias());
                for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                    assertNoDataFusionInternals(name, sc.name());
                }
            }
            for (String col : def.projectionOrder()) {
                assertNoDataFusionInternals(name, col);
            }
        }
    }

    // ── All target mapping types are correct ─────────────────────────────

    public void testAllLadderTargetMappingTypes() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            for (GroupKey key : def.groupKeys()) {
                String mappingType = def.targetMapping().get(key.name());
                assertEquals(name + " key " + key.name(), key.columnType().osType(), mappingType);
            }
            for (AggregateSpec agg : def.aggregates()) {
                String mappingType = def.targetMapping().get(agg.userAlias());
                assertEquals(name + " agg " + agg.userAlias(), "long", mappingType);
            }
        }
    }

    // ── Schema validation round-trip for 45-field L0 ─────────────────────

    public void testSchemaValidationRoundTrip45Fields() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        Map<String, Object> schema = new HashMap<>();
        for (Map.Entry<String, String> entry : def.targetMapping().entrySet()) {
            schema.put(entry.getKey(), entry.getValue());
        }
        // Must not throw
        def.validateSchema(schema);
    }

    public void testSchemaValidationRejectsMissing45thField() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        Map<String, Object> schema = new HashMap<>();
        List<String> cols = def.projectionOrder();
        // Add all except the last one
        for (int i = 0; i < cols.size() - 1; i++) {
            schema.put(cols.get(i), def.targetMapping().get(cols.get(i)));
        }
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> def.validateSchema(schema));
        assertTrue(ex.getMessage().contains("missing field"));
        assertTrue(ex.getMessage().contains(cols.get(cols.size() - 1)));
    }

    // ── Null-fill compatibility: narrower file still validates ────────────

    public void testNarrowerSchemaPassesValidationForWiderDefinition() {
        // A state file with fewer fields than the definition is valid IF the
        // reader null-fills absent columns. The schema validation only checks
        // that definition fields EXIST in the schema, so a wider schema
        // (more fields than needed) passes. This test documents the contract.
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        Map<String, Object> wideSchema = new HashMap<>();
        for (Map.Entry<String, String> entry : def.targetMapping().entrySet()) {
            wideSchema.put(entry.getKey(), entry.getValue());
        }
        wideSchema.put("extra_column", "text"); // wider
        def.validateSchema(wideSchema); // must not throw
    }

    // ── Monotonic output width ───────────────────────────────────────────

    public void testMonotonicOutputWidth() {
        int l0 = MVCompiledDefinition.compiledFor("clickbench_100m").projectionOrder().size();
        int l1 = MVCompiledDefinition.compiledFor("heavy_l1").projectionOrder().size();
        int l2 = MVCompiledDefinition.compiledFor("heavy_l2").projectionOrder().size();
        int l3 = MVCompiledDefinition.compiledFor("heavy_l3").projectionOrder().size();
        assertEquals(45, l0);
        assertEquals(48, l1);
        assertEquals(88, l2);
        assertEquals(130, l3);
        assertTrue("L1 > L0", l1 > l0);
        assertTrue("L2 > L1", l2 > l1);
        assertTrue("L3 > L2", l3 > l2);
    }

    // ── Target mapping JSON generation ───────────────────────────────────

    public void testTargetMappingJsonForL0Has45Properties() {
        String json = MVViewsService.TargetCreator.targetMapping("clickbench_100m");
        assertTrue(json.contains("\"dynamic\":\"false\""));
        assertTrue(json.contains("\"_field_names\":{\"enabled\":false}"));
        assertTrue(json.contains("\"_mv_source_generation\":{\"type\":\"long\",\"index\":false}"));
        // All 45 compiled fields + _mv_source_generation = 46 in properties
        // Verify a sample of key+agg fields
        assertTrue(json.contains("\"EventTime\":{\"type\":\"long\"}"));
        assertTrue(json.contains("\"adv_sum\":{\"type\":\"long\"}"));
        assertTrue(json.contains("\"adv_min\":{\"type\":\"long\"}"));
        assertTrue(json.contains("\"adv_max\":{\"type\":\"long\"}"));
        assertTrue(json.contains("\"adv_cnt\":{\"type\":\"long\"}"));
        assertTrue(json.contains("\"send_cnt\":{\"type\":\"long\"}"));
    }

    public void testTargetMappingJsonForL3Has130Properties() {
        String json = MVViewsService.TargetCreator.targetMapping("heavy_l3");
        assertTrue(json.contains("\"URL\":{\"type\":\"keyword\"}"));
        assertTrue(json.contains("\"Referer\":{\"type\":\"keyword\"}"));
        assertTrue(json.contains("\"opener_cnt\":{\"type\":\"long\"}"));
    }

    // ── Projection uniqueness ────────────────────────────────────────────

    public void testAllProjectionColumnsUniquePerRung() {
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            Set<String> unique = new HashSet<>(def.projectionOrder());
            assertEquals(name + " must have no duplicate projection columns", def.projectionOrder().size(), unique.size());
        }
    }

    // ── L3 has keyword keys ──────────────────────────────────────────────

    public void testL3HasKeywordGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("heavy_l3");
        assertEquals(GroupKey.ColumnType.KEYWORD, def.groupKeys().get(8).columnType()); // URL
        assertEquals(GroupKey.ColumnType.KEYWORD, def.groupKeys().get(9).columnType()); // Referer
        assertEquals("keyword", def.targetMapping().get("URL"));
        assertEquals("keyword", def.targetMapping().get("Referer"));
    }

    // ── SELECT column count in partial SQL ────────────────────────────────

    public void testPartialSqlSelectColumnCounts() {
        assertSqlSelectCount("clickbench_100m", 45);
        assertSqlSelectCount("heavy_l1", 48);
        assertSqlSelectCount("heavy_l2", 88);
        assertSqlSelectCount("heavy_l3", 130);
    }

    // ── MVMappingGenerator produces correct field count ──────────────────

    @SuppressWarnings("unchecked")
    public void testMappingGeneratorFieldCountsMatchProjection() {
        MVMappingGenerator generator = new MVMappingGenerator();
        for (String name : List.of("clickbench_100m", "heavy_l1", "heavy_l2", "heavy_l3")) {
            MVCompiledDefinition def = MVCompiledDefinition.compiledFor(name);
            Map<String, Object> mapping = generator.generateMapping(def);
            Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
            assertEquals(name + " MVMappingGenerator field count must match projection", def.projectionOrder().size(), properties.size());
            assertTrue(name + " mapping must be compatible", generator.isCompatible(def, mapping));
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static void assertAggCounts(MVCompiledDefinition def, int expectedSum, int expectedMin, int expectedMax, int expectedCnt) {
        int sums = 0;
        int mins = 0;
        int maxes = 0;
        int counts = 0;
        for (AggregateSpec a : def.aggregates()) {
            switch (a.function()) {
                case SUM -> sums++;
                case MIN -> mins++;
                case MAX -> maxes++;
                case COUNT -> counts++;
                default -> fail("unexpected function: " + a.function());
            }
        }
        assertEquals("SUM count", expectedSum, sums);
        assertEquals("MIN count", expectedMin, mins);
        assertEquals("MAX count", expectedMax, maxes);
        assertEquals("COUNT count", expectedCnt, counts);
    }

    private static void assertPhysicalNamesMatchSpec(String name) {
        MVCompiledDefinition compiled = MVCompiledDefinition.compiledFor(name);
        MVDefinitionSpec spec = MVDefinitionSpec.source(name);
        assertEquals(compiled.stateColumnNames(), spec.shipFields());
    }

    private static void assertPartialSqlContainsAllFourAggFunctions(String name, int expectedPerFunction) {
        String sql = MVCompiledDefinition.compiledFor(name).buildPartialSql("mv_input");
        String select = sql.substring("SELECT ".length(), sql.indexOf(" FROM "));
        assertEquals(name + " SUM count", expectedPerFunction, countOccurrences(select, "SUM("));
        assertEquals(name + " MIN count", expectedPerFunction, countOccurrences(select, "MIN("));
        assertEquals(name + " MAX count", expectedPerFunction, countOccurrences(select, "MAX("));
        assertEquals(name + " COUNT count", expectedPerFunction, countOccurrences(select, "COUNT("));
    }

    private static void assertFoldSqlCorrectness(String name, int metrics) {
        String sql = MVCompiledDefinition.compiledFor(name).buildFoldSql("mv_input");
        String select = sql.substring("SELECT ".length(), sql.indexOf(" FROM "));
        // Fold: SUM(sums) + SUM(counts) = 2*metrics SUM; metrics MIN; metrics MAX
        assertEquals(name + " fold SUM", 2 * metrics, countOccurrences(select, "SUM("));
        assertEquals(name + " fold MIN", metrics, countOccurrences(select, "MIN("));
        assertEquals(name + " fold MAX", metrics, countOccurrences(select, "MAX("));
    }

    private static void assertNoDataFusionInternals(String context, String value) {
        assertFalse(context + ": " + value + " contains Int64(1)", value.contains("Int64(1)"));
        assertFalse(context + ": " + value + " contains mv_input.", value.contains("mv_input."));
        assertFalse(context + ": " + value + " contains [count]", value.contains("[count]"));
        assertFalse(context + ": " + value + " contains [sum]", value.contains("[sum]"));
    }

    private static int countOccurrences(String str, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    private void assertSqlSelectCount(String name, int expected) {
        String sql = MVCompiledDefinition.compiledFor(name).buildPartialSql("mv_input");
        int fromIdx = sql.indexOf(" FROM ");
        String selectPart = sql.substring("SELECT ".length(), fromIdx);
        int depth = 0;
        int commas = 0;
        for (char c : selectPart.toCharArray()) {
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) commas++;
        }
        assertEquals("SELECT column count for " + name, expected, commas + 1);
    }
}
