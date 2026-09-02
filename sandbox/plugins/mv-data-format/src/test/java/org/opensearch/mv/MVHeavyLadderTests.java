/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for the heavy-MV saturation ladder definitions (L1, L2, L3) in
 * {@link MVDefinitionSpec}. Validates:
 * <ul>
 *   <li>Definition lookup by name (source + fold)</li>
 *   <li>Output column counts (group keys + aggregates)</li>
 *   <li>Ship-field uniqueness</li>
 *   <li>Group-key ordering and type correctness</li>
 *   <li>SQL structural validity (SELECT/GROUP BY, aggregate function counts)</li>
 *   <li>Source-fold mapping/projection agreement</li>
 *   <li>Stable definition hash across calls</li>
 *   <li>Monotonic width increase across ladder rungs</li>
 *   <li>All numeric metric types (SUM/MIN/MAX/COUNT)</li>
 * </ul>
 */
public class MVHeavyLadderTests extends OpenSearchTestCase {

    // ── L1: 8 keys + 10 metrics × 4 = 48 outputs ─────────────────────

    public void testL1NamedLookupSource() {
        assertSame(MVDefinitionSpec.HEAVY_L1, MVDefinitionSpec.source("heavy_l1"));
    }

    public void testL1NamedLookupFold() {
        assertSame(MVDefinitionSpec.HEAVY_L1_FOLD, MVDefinitionSpec.fold("heavy_l1"));
    }

    public void testL1SourceColumnCount() {
        MVDefinitionSpec s = MVDefinitionSpec.HEAVY_L1;
        // 8 group keys + 10 metric source fields = 18 captured columns
        assertEquals(18, s.columns().size());
    }

    public void testL1GroupKeyCount() {
        assertEquals(8, MVDefinitionSpec.HEAVY_L1.groupKeys());
    }

    public void testL1GroupKeyOrder() {
        MVDefinitionSpec s = MVDefinitionSpec.HEAVY_L1;
        List<String> expected = List.of("EventTime", "RegionID", "OS", "CounterID", "IsRefresh", "UserID", "WatchID", "FUniqID");
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), s.columns().get(i).name());
        }
    }

    public void testL1ShipFieldCount() {
        // 8 keys + (SUM+MIN+MAX+COUNT) × 10 = 48
        assertEquals(48, MVDefinitionSpec.HEAVY_L1.shipFields().size());
    }

    public void testL1ShipFieldsUnique() {
        Set<String> unique = new HashSet<>(MVDefinitionSpec.HEAVY_L1.shipFields());
        assertEquals(48, unique.size());
    }

    public void testL1AllColumnsInt64() {
        for (MVDefinitionSpec.Column c : MVDefinitionSpec.HEAVY_L1.columns()) {
            assertEquals("all L1 columns are INT64: " + c.name(), MVDefinitionSpec.ColumnType.INT64, c.type());
        }
    }

    public void testL1SourceSqlStructure() {
        String sql = MVDefinitionSpec.HEAVY_L1.sql();
        assertTrue(sql.startsWith("SELECT "));
        assertTrue(sql.contains("FROM mv_input"));
        assertTrue(sql.contains("GROUP BY"));
        // Must reference all 8 group keys
        for (String k : List.of("EventTime", "RegionID", "OS", "CounterID", "IsRefresh", "UserID", "WatchID", "FUniqID")) {
            assertTrue("SQL must reference key " + k, sql.contains("\"" + k + "\""));
        }
    }

    public void testL1SourceSqlAggregateCounts() {
        String sql = MVDefinitionSpec.HEAVY_L1.sql();
        assertEquals(10, countOccurrences(sql, "SUM("));
        assertEquals(10, countOccurrences(sql, "MIN("));
        assertEquals(10, countOccurrences(sql, "MAX("));
        assertEquals(10, countOccurrences(sql, "COUNT("));
    }

    public void testL1FoldColumnCount() {
        // Fold columns = all ship fields = 48
        assertEquals(48, MVDefinitionSpec.HEAVY_L1_FOLD.columns().size());
    }

    public void testL1FoldShipFieldsMatchSource() {
        assertEquals(MVDefinitionSpec.HEAVY_L1.shipFields(), MVDefinitionSpec.HEAVY_L1_FOLD.shipFields());
    }

    public void testL1FoldColumnsMatchShipFields() {
        MVDefinitionSpec fold = MVDefinitionSpec.HEAVY_L1_FOLD;
        for (int i = 0; i < fold.columns().size(); i++) {
            assertEquals(fold.shipFields().get(i), fold.columns().get(i).name());
        }
    }

    public void testL1FoldSqlAggregates() {
        String sql = MVDefinitionSpec.HEAVY_L1_FOLD.sql();
        // Fold: SUM(sums) + SUM(counts) = 20 SUM, 10 MIN, 10 MAX
        assertEquals(20, countOccurrences(sql, "SUM("));
        assertEquals(10, countOccurrences(sql, "MIN("));
        assertEquals(10, countOccurrences(sql, "MAX("));
    }

    public void testL1HashStability() {
        assertEquals(MVDefinitionSpec.HEAVY_L1.hashCode(), MVDefinitionSpec.HEAVY_L1.hashCode());
        assertEquals(MVDefinitionSpec.HEAVY_L1_FOLD.hashCode(), MVDefinitionSpec.HEAVY_L1_FOLD.hashCode());
    }

    // ── L2: 8 keys + 20 metrics × 4 = 88 outputs ─────────────────────

    public void testL2NamedLookupSource() {
        assertSame(MVDefinitionSpec.HEAVY_L2, MVDefinitionSpec.source("heavy_l2"));
    }

    public void testL2NamedLookupFold() {
        assertSame(MVDefinitionSpec.HEAVY_L2_FOLD, MVDefinitionSpec.fold("heavy_l2"));
    }

    public void testL2SourceColumnCount() {
        // 8 keys + 20 metrics = 28
        assertEquals(28, MVDefinitionSpec.HEAVY_L2.columns().size());
    }

    public void testL2GroupKeyCount() {
        assertEquals(8, MVDefinitionSpec.HEAVY_L2.groupKeys());
    }

    public void testL2GroupKeySameAsL1() {
        // L2 shares the same 8 group keys as L1
        MVDefinitionSpec l1 = MVDefinitionSpec.HEAVY_L1;
        MVDefinitionSpec l2 = MVDefinitionSpec.HEAVY_L2;
        assertEquals(l1.groupKeys(), l2.groupKeys());
        for (int i = 0; i < l1.groupKeys(); i++) {
            assertEquals(l1.columns().get(i).name(), l2.columns().get(i).name());
            assertEquals(l1.columns().get(i).type(), l2.columns().get(i).type());
        }
    }

    public void testL2ShipFieldCount() {
        // 8 + 20×4 = 88
        assertEquals(88, MVDefinitionSpec.HEAVY_L2.shipFields().size());
    }

    public void testL2ShipFieldsUnique() {
        Set<String> unique = new HashSet<>(MVDefinitionSpec.HEAVY_L2.shipFields());
        assertEquals(88, unique.size());
    }

    public void testL2SourceSqlAggregateCounts() {
        String sql = MVDefinitionSpec.HEAVY_L2.sql();
        assertEquals(20, countOccurrences(sql, "SUM("));
        assertEquals(20, countOccurrences(sql, "MIN("));
        assertEquals(20, countOccurrences(sql, "MAX("));
        assertEquals(20, countOccurrences(sql, "COUNT("));
    }

    public void testL2FoldColumnCount() {
        assertEquals(88, MVDefinitionSpec.HEAVY_L2_FOLD.columns().size());
    }

    public void testL2FoldShipFieldsMatchSource() {
        assertEquals(MVDefinitionSpec.HEAVY_L2.shipFields(), MVDefinitionSpec.HEAVY_L2_FOLD.shipFields());
    }

    public void testL2FoldSqlAggregates() {
        String sql = MVDefinitionSpec.HEAVY_L2_FOLD.sql();
        // 20 SUM(sums) + 20 SUM(counts) = 40 SUM, 20 MIN, 20 MAX
        assertEquals(40, countOccurrences(sql, "SUM("));
        assertEquals(20, countOccurrences(sql, "MIN("));
        assertEquals(20, countOccurrences(sql, "MAX("));
    }

    public void testL2FoldColumnsMatchShipFields() {
        MVDefinitionSpec fold = MVDefinitionSpec.HEAVY_L2_FOLD;
        for (int i = 0; i < fold.columns().size(); i++) {
            assertEquals(fold.shipFields().get(i), fold.columns().get(i).name());
        }
    }

    public void testL2HashStability() {
        assertEquals(MVDefinitionSpec.HEAVY_L2.hashCode(), MVDefinitionSpec.HEAVY_L2.hashCode());
    }

    // ── L3: 10 keys (2 UTF8) + 30 metrics × 4 = 130 outputs ──────────

    public void testL3NamedLookupSource() {
        assertSame(MVDefinitionSpec.HEAVY_L3, MVDefinitionSpec.source("heavy_l3"));
    }

    public void testL3NamedLookupFold() {
        assertSame(MVDefinitionSpec.HEAVY_L3_FOLD, MVDefinitionSpec.fold("heavy_l3"));
    }

    public void testL3SourceColumnCount() {
        // 10 keys + 30 metrics = 40
        assertEquals(40, MVDefinitionSpec.HEAVY_L3.columns().size());
    }

    public void testL3GroupKeyCount() {
        assertEquals(10, MVDefinitionSpec.HEAVY_L3.groupKeys());
    }

    public void testL3GroupKeyOrder() {
        MVDefinitionSpec s = MVDefinitionSpec.HEAVY_L3;
        List<String> expected = List.of(
            "EventTime",
            "RegionID",
            "OS",
            "CounterID",
            "IsRefresh",
            "UserID",
            "WatchID",
            "FUniqID",
            "URL",
            "Referer"
        );
        for (int i = 0; i < expected.size(); i++) {
            assertEquals(expected.get(i), s.columns().get(i).name());
        }
    }

    public void testL3HasUtf8GroupKeys() {
        MVDefinitionSpec s = MVDefinitionSpec.HEAVY_L3;
        assertEquals(MVDefinitionSpec.ColumnType.UTF8, s.columns().get(8).type()); // URL
        assertEquals(MVDefinitionSpec.ColumnType.UTF8, s.columns().get(9).type()); // Referer
    }

    public void testL3ShipFieldCount() {
        // 10 + 30×4 = 130
        assertEquals(130, MVDefinitionSpec.HEAVY_L3.shipFields().size());
    }

    public void testL3ShipFieldsUnique() {
        Set<String> unique = new HashSet<>(MVDefinitionSpec.HEAVY_L3.shipFields());
        assertEquals(130, unique.size());
    }

    public void testL3SourceSqlAggregateCounts() {
        String sql = MVDefinitionSpec.HEAVY_L3.sql();
        assertEquals(30, countOccurrences(sql, "SUM("));
        assertEquals(30, countOccurrences(sql, "MIN("));
        assertEquals(30, countOccurrences(sql, "MAX("));
        assertEquals(30, countOccurrences(sql, "COUNT("));
    }

    public void testL3FoldColumnCount() {
        assertEquals(130, MVDefinitionSpec.HEAVY_L3_FOLD.columns().size());
    }

    public void testL3FoldShipFieldsMatchSource() {
        assertEquals(MVDefinitionSpec.HEAVY_L3.shipFields(), MVDefinitionSpec.HEAVY_L3_FOLD.shipFields());
    }

    public void testL3FoldSqlAggregates() {
        String sql = MVDefinitionSpec.HEAVY_L3_FOLD.sql();
        // 30 SUM(sums) + 30 SUM(counts) = 60 SUM, 30 MIN, 30 MAX
        assertEquals(60, countOccurrences(sql, "SUM("));
        assertEquals(30, countOccurrences(sql, "MIN("));
        assertEquals(30, countOccurrences(sql, "MAX("));
    }

    public void testL3FoldColumnsMatchShipFields() {
        MVDefinitionSpec fold = MVDefinitionSpec.HEAVY_L3_FOLD;
        for (int i = 0; i < fold.columns().size(); i++) {
            assertEquals(fold.shipFields().get(i), fold.columns().get(i).name());
        }
    }

    public void testL3FoldUtf8KeysPreserved() {
        MVDefinitionSpec fold = MVDefinitionSpec.HEAVY_L3_FOLD;
        assertEquals(MVDefinitionSpec.ColumnType.UTF8, fold.columns().get(8).type()); // URL
        assertEquals(MVDefinitionSpec.ColumnType.UTF8, fold.columns().get(9).type()); // Referer
    }

    public void testL3HashStability() {
        assertEquals(MVDefinitionSpec.HEAVY_L3.hashCode(), MVDefinitionSpec.HEAVY_L3.hashCode());
    }

    // ── Monotonic width increase across ladder ────────────────────────

    public void testLadderOutputWidthMonotonicallyIncreases() {
        int l0 = MVDefinitionSpec.CLICKBENCH_100M.shipFields().size();    // 45
        int l1 = MVDefinitionSpec.HEAVY_L1.shipFields().size();           // 48
        int l2 = MVDefinitionSpec.HEAVY_L2.shipFields().size();           // 88
        int l3 = MVDefinitionSpec.HEAVY_L3.shipFields().size();           // 130

        assertEquals(45, l0);
        assertEquals(48, l1);
        assertEquals(88, l2);
        assertEquals(130, l3);
        assertTrue("L1 > L0", l1 > l0);
        assertTrue("L2 > L1", l2 > l1);
        assertTrue("L3 > L2", l3 > l2);
    }

    public void testLadderGroupKeyWidthMonotonicallyIncreases() {
        int l0 = MVDefinitionSpec.CLICKBENCH_100M.groupKeys();  // 5
        int l1 = MVDefinitionSpec.HEAVY_L1.groupKeys();         // 8
        int l2 = MVDefinitionSpec.HEAVY_L2.groupKeys();         // 8
        int l3 = MVDefinitionSpec.HEAVY_L3.groupKeys();         // 10

        assertTrue("L1 >= L0", l1 >= l0);
        assertTrue("L2 >= L1", l2 >= l1);
        assertTrue("L3 >= L2", l3 >= l2);
    }

    // ── All rungs carry EventTime in group keys ───────────────────────

    public void testAllLadderRungsHaveEventTimeAsFirstKey() {
        assertEquals("EventTime", MVDefinitionSpec.CLICKBENCH_100M.columns().get(0).name());
        assertEquals("EventTime", MVDefinitionSpec.HEAVY_L1.columns().get(0).name());
        assertEquals("EventTime", MVDefinitionSpec.HEAVY_L2.columns().get(0).name());
        assertEquals("EventTime", MVDefinitionSpec.HEAVY_L3.columns().get(0).name());
    }

    // ── Ship field naming pattern (prefix_sum/min/max/cnt) ────────────

    public void testShipFieldNamingConvention() {
        // Every non-key ship field must end with _sum, _min, _max, or _cnt
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            MVDefinitionSpec spec = MVDefinitionSpec.source(name);
            List<String> aggFields = spec.shipFields().subList(spec.groupKeys(), spec.shipFields().size());
            for (String f : aggFields) {
                assertTrue(
                    "ship field '" + f + "' in " + name + " must end with _sum/_min/_max/_cnt",
                    f.endsWith("_sum") || f.endsWith("_min") || f.endsWith("_max") || f.endsWith("_cnt")
                );
            }
        }
    }

    public void testShipFieldAggregateGroups() {
        // Every 4 consecutive aggregate ship fields must share the same prefix
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            MVDefinitionSpec spec = MVDefinitionSpec.source(name);
            List<String> aggFields = spec.shipFields().subList(spec.groupKeys(), spec.shipFields().size());
            assertEquals("aggregate fields must be multiple of 4", 0, aggFields.size() % 4);
            for (int i = 0; i < aggFields.size(); i += 4) {
                String sumF = aggFields.get(i);
                String minF = aggFields.get(i + 1);
                String maxF = aggFields.get(i + 2);
                String cntF = aggFields.get(i + 3);
                String prefix = sumF.substring(0, sumF.lastIndexOf('_'));
                assertEquals(prefix + "_sum", sumF);
                assertEquals(prefix + "_min", minF);
                assertEquals(prefix + "_max", maxF);
                assertEquals(prefix + "_cnt", cntF);
            }
        }
    }

    // ── All four numeric metric types present ─────────────────────────

    public void testAllSourceSqlContainAllFourAggTypes() {
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            String sql = MVDefinitionSpec.source(name).sql();
            assertTrue(name + " must have SUM", sql.contains("SUM("));
            assertTrue(name + " must have MIN", sql.contains("MIN("));
            assertTrue(name + " must have MAX", sql.contains("MAX("));
            assertTrue(name + " must have COUNT", sql.contains("COUNT("));
        }
    }

    // ── SQL FROM clause always references mv_input ────────────────────

    public void testAllSqlReferenceMvInput() {
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            assertTrue(name + " source", MVDefinitionSpec.source(name).sql().contains("FROM mv_input"));
            assertTrue(name + " fold", MVDefinitionSpec.fold(name).sql().contains("FROM mv_input"));
        }
    }

    // ── Source/fold SQL are different ──────────────────────────────────

    public void testSourceAndFoldSqlDiffer() {
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            assertNotEquals(
                name + " source and fold SQL must differ",
                MVDefinitionSpec.source(name).sql(),
                MVDefinitionSpec.fold(name).sql()
            );
        }
    }

    // ── allNames() enumerates all definitions ─────────────────────────

    public void testAllNamesIncludesLadder() {
        List<String> names = MVDefinitionSpec.allNames();
        assertTrue(names.contains("heavy_l1"));
        assertTrue(names.contains("heavy_l2"));
        assertTrue(names.contains("heavy_l3"));
        assertTrue(names.contains("clickbench_100m"));
    }

    public void testAllNamesSourceLookup() {
        for (String name : MVDefinitionSpec.allNames()) {
            assertNotNull("source lookup for " + name, MVDefinitionSpec.source(name));
        }
    }

    public void testAllNamesFoldLookup() {
        for (String name : MVDefinitionSpec.allNames()) {
            assertNotNull("fold lookup for " + name, MVDefinitionSpec.fold(name));
        }
    }

    // ── Cross-rung uniqueness: no duplicate prefixes across levels ────

    public void testNoDuplicatePrefixesWithinRung() {
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            MVDefinitionSpec spec = MVDefinitionSpec.source(name);
            List<String> aggFields = spec.shipFields().subList(spec.groupKeys(), spec.shipFields().size());
            Set<String> prefixes = new HashSet<>();
            for (int i = 0; i < aggFields.size(); i += 4) {
                String prefix = aggFields.get(i).substring(0, aggFields.get(i).lastIndexOf('_'));
                assertTrue("duplicate prefix '" + prefix + "' in " + name, prefixes.add(prefix));
            }
        }
    }

    // ── L1 metrics are a subset of L2, which is a subset of L3 ───────

    public void testMetricSubsetRelationship() {
        Set<String> l1Metrics = extractMetricSourceFields(MVDefinitionSpec.HEAVY_L1);
        Set<String> l2Metrics = extractMetricSourceFields(MVDefinitionSpec.HEAVY_L2);
        Set<String> l3Metrics = extractMetricSourceFields(MVDefinitionSpec.HEAVY_L3);

        assertTrue("L1 metrics ⊂ L2", l2Metrics.containsAll(l1Metrics));
        assertTrue("L2 metrics ⊂ L3", l3Metrics.containsAll(l2Metrics));
        assertEquals(10, l1Metrics.size());
        assertEquals(20, l2Metrics.size());
        assertEquals(30, l3Metrics.size());
    }

    // ── SQL does not contain unsupported functions ─────────────────────

    public void testNoUnsupportedAggregates() {
        for (String name : List.of("heavy_l1", "heavy_l2", "heavy_l3")) {
            String sourceSql = MVDefinitionSpec.source(name).sql().toUpperCase(java.util.Locale.ROOT);
            String foldSql = MVDefinitionSpec.fold(name).sql().toUpperCase(java.util.Locale.ROOT);
            for (String bad : List.of("AVG(", "STDDEV(", "VARIANCE(", "MEDIAN(", "PERCENTILE(")) {
                assertFalse(name + " source must not contain " + bad, sourceSql.contains(bad));
                assertFalse(name + " fold must not contain " + bad, foldSql.contains(bad));
            }
        }
    }

    // ── SQL SELECT column count matches expected ──────────────────────

    public void testSourceSqlSelectColumnCount() {
        // Count comma-separated expressions between SELECT and FROM
        assertSqlSelectCount("heavy_l1", 48); // 8 keys + 40 agg expressions
        assertSqlSelectCount("heavy_l2", 88); // 8 keys + 80 agg expressions
        assertSqlSelectCount("heavy_l3", 130); // 10 keys + 120 agg expressions
    }

    // ── Helpers ───────────────────────────────────────────────────────

    private static int countOccurrences(String str, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }

    private static Set<String> extractMetricSourceFields(MVDefinitionSpec spec) {
        Set<String> metrics = new HashSet<>();
        for (int i = spec.groupKeys(); i < spec.columns().size(); i++) {
            metrics.add(spec.columns().get(i).name());
        }
        return metrics;
    }

    private void assertSqlSelectCount(String name, int expected) {
        String sql = MVDefinitionSpec.source(name).sql();
        // Extract the SELECT ... FROM portion
        int fromIdx = sql.indexOf("FROM mv_input");
        assertTrue("SQL must contain FROM mv_input", fromIdx > 0);
        String selectPart = sql.substring("SELECT ".length(), fromIdx).trim();
        // Count top-level commas (not inside parens)
        int depth = 0;
        int commas = 0;
        for (char c : selectPart.toCharArray()) {
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) commas++;
        }
        int columnCount = commas + 1;
        assertEquals("SELECT column count for " + name, expected, columnCount);
    }
}
