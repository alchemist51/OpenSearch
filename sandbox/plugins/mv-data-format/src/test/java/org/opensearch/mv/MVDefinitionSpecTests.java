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
 * Tests for {@link MVDefinitionSpec} — particularly the wide-column
 * CLICKBENCH_100M source and fold definitions used for the 100M catch-up
 * benchmark. Validates column counts, group key positions, alias stability,
 * SQL structure, and deterministic field ordering.
 */
public class MVDefinitionSpecTests extends OpenSearchTestCase {

    // ── CLICKBENCH_100M source ─────────────────────────────────────────

    public void testClickbench100mSourceHas15Columns() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertEquals("source captures 15 columns (5 keys + 10 metrics)", 15, spec.columns().size());
    }

    public void testClickbench100mSourceGroupKeys() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertEquals(5, spec.groupKeys());
        assertEquals("EventTime", spec.columns().get(0).name());
        assertEquals("RegionID", spec.columns().get(1).name());
        assertEquals("OS", spec.columns().get(2).name());
        assertEquals("CounterID", spec.columns().get(3).name());
        assertEquals("IsRefresh", spec.columns().get(4).name());
    }

    public void testClickbench100mSourceMetricColumns() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        List<String> expectedMetrics = List.of(
            "AdvEngineID",
            "ResolutionWidth",
            "ResolutionHeight",
            "ResolutionDepth",
            "ClientIP",
            "RemoteIP",
            "ConnectTiming",
            "DNSTiming",
            "FetchTiming",
            "SendTiming"
        );
        for (int i = 0; i < expectedMetrics.size(); i++) {
            assertEquals(expectedMetrics.get(i), spec.columns().get(5 + i).name());
            assertEquals(MVDefinitionSpec.ColumnType.INT64, spec.columns().get(5 + i).type());
        }
    }

    public void testClickbench100mSourceShipFieldsHas45Entries() {
        // 5 group keys + (SUM + MIN + MAX + COUNT) × 10 = 45
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertEquals(45, spec.shipFields().size());
    }

    public void testClickbench100mSourceShipFieldsGroupKeysFirst() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertEquals("EventTime", spec.shipFields().get(0));
        assertEquals("RegionID", spec.shipFields().get(1));
        assertEquals("OS", spec.shipFields().get(2));
        assertEquals("CounterID", spec.shipFields().get(3));
        assertEquals("IsRefresh", spec.shipFields().get(4));
    }

    public void testClickbench100mSourceShipFieldsAggregatePattern() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        // After 5 group keys, each metric has 4 aggregates: sum, min, max, cnt
        // First metric (AdvEngineID) → adv_sum, adv_min, adv_max, adv_cnt
        assertEquals("adv_sum", spec.shipFields().get(5));
        assertEquals("adv_min", spec.shipFields().get(6));
        assertEquals("adv_max", spec.shipFields().get(7));
        assertEquals("adv_cnt", spec.shipFields().get(8));
        // Second metric (ResolutionWidth) → resw_sum, resw_min, resw_max, resw_cnt
        assertEquals("resw_sum", spec.shipFields().get(9));
        assertEquals("resw_min", spec.shipFields().get(10));
        assertEquals("resw_max", spec.shipFields().get(11));
        assertEquals("resw_cnt", spec.shipFields().get(12));
        // Last metric (SendTiming) → send_sum, send_min, send_max, send_cnt
        assertEquals("send_sum", spec.shipFields().get(41));
        assertEquals("send_min", spec.shipFields().get(42));
        assertEquals("send_max", spec.shipFields().get(43));
        assertEquals("send_cnt", spec.shipFields().get(44));
    }

    public void testClickbench100mSourceShipFieldsAllUnique() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        Set<String> unique = new HashSet<>(spec.shipFields());
        assertEquals("all 45 ship fields must be unique", 45, unique.size());
    }

    public void testClickbench100mSourceAllColumnsAreInt64() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        for (MVDefinitionSpec.Column col : spec.columns()) {
            assertEquals("all source columns must be INT64: " + col.name(), MVDefinitionSpec.ColumnType.INT64, col.type());
        }
    }

    public void testClickbench100mSourceSqlContainsGroupBy() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertTrue(spec.sql().contains("GROUP BY"));
        assertTrue(spec.sql().contains("\"EventTime\""));
        assertTrue(spec.sql().contains("\"RegionID\""));
        assertTrue(spec.sql().contains("\"OS\""));
        assertTrue(spec.sql().contains("\"CounterID\""));
        assertTrue(spec.sql().contains("\"IsRefresh\""));
    }

    public void testClickbench100mSourceSqlHas40Aggregates() {
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        // Each of 10 metrics has SUM, MIN, MAX, COUNT = 40 aggregate calls
        int sumCount = countOccurrences(spec.sql(), "SUM(");
        int minCount = countOccurrences(spec.sql(), "MIN(");
        int maxCount = countOccurrences(spec.sql(), "MAX(");
        int cntCount = countOccurrences(spec.sql(), "COUNT(");
        assertEquals("10 SUM aggregates", 10, sumCount);
        assertEquals("10 MIN aggregates", 10, minCount);
        assertEquals("10 MAX aggregates", 10, maxCount);
        assertEquals("10 COUNT aggregates", 10, cntCount);
    }

    public void testClickbench100mNamedLookupSource() {
        MVDefinitionSpec spec = MVDefinitionSpec.source("clickbench_100m");
        assertSame(MVDefinitionSpec.CLICKBENCH_100M, spec);
    }

    // ── CLICKBENCH_100M fold ──────────────────────────────────────────

    public void testClickbench100mFoldHas45Columns() {
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        assertEquals(45, fold.columns().size());
    }

    public void testClickbench100mFoldGroupKeys() {
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        assertEquals(5, fold.groupKeys());
        assertEquals("EventTime", fold.columns().get(0).name());
        assertEquals("RegionID", fold.columns().get(1).name());
        assertEquals("OS", fold.columns().get(2).name());
        assertEquals("CounterID", fold.columns().get(3).name());
        assertEquals("IsRefresh", fold.columns().get(4).name());
    }

    public void testClickbench100mFoldShipFieldsMatch() {
        MVDefinitionSpec source = MVDefinitionSpec.CLICKBENCH_100M;
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        assertEquals("fold ship fields must match source ship fields", source.shipFields(), fold.shipFields());
    }

    public void testClickbench100mFoldSqlUsesCorrectAggregates() {
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        // Fold: SUM sums and counts, MIN mins, MAX maxes (quoted identifiers)
        assertTrue("fold SQL must SUM sums", fold.sql().contains("SUM(\"adv_sum\")"));
        assertTrue("fold SQL must MIN mins", fold.sql().contains("MIN(\"adv_min\")"));
        assertTrue("fold SQL must MAX maxes", fold.sql().contains("MAX(\"adv_max\")"));
        assertTrue("fold SQL must SUM counts", fold.sql().contains("SUM(\"adv_cnt\")"));
    }

    public void testClickbench100mFoldSqlAggregateCountsCorrect() {
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        // SUM: 10 (sums) + 10 (counts) = 20 SUM calls in fold
        int sumCount = countOccurrences(fold.sql(), "SUM(");
        int minCount = countOccurrences(fold.sql(), "MIN(");
        int maxCount = countOccurrences(fold.sql(), "MAX(");
        assertEquals("20 SUM calls in fold (sums + counts)", 20, sumCount);
        assertEquals("10 MIN calls in fold", 10, minCount);
        assertEquals("10 MAX calls in fold", 10, maxCount);
    }

    public void testClickbench100mNamedLookupFold() {
        MVDefinitionSpec fold = MVDefinitionSpec.fold("clickbench_100m");
        assertSame(MVDefinitionSpec.CLICKBENCH_100M_FOLD, fold);
    }

    // ── Source/fold state order alignment ──────────────────────────────

    public void testClickbench100mSourceAndFoldShipFieldsDeterministic() {
        // Repeated calls must return identical lists (immutability)
        MVDefinitionSpec s1 = MVDefinitionSpec.source("clickbench_100m");
        MVDefinitionSpec s2 = MVDefinitionSpec.source("clickbench_100m");
        assertEquals(s1.shipFields(), s2.shipFields());

        MVDefinitionSpec f1 = MVDefinitionSpec.fold("clickbench_100m");
        MVDefinitionSpec f2 = MVDefinitionSpec.fold("clickbench_100m");
        assertEquals(f1.shipFields(), f2.shipFields());
    }

    public void testClickbench100mFoldColumnsMatchShipFields() {
        MVDefinitionSpec fold = MVDefinitionSpec.CLICKBENCH_100M_FOLD;
        // Fold columns = all 45 state columns (keys + aggregates)
        for (int i = 0; i < fold.columns().size(); i++) {
            assertEquals("fold column " + i + " must match ship field", fold.shipFields().get(i), fold.columns().get(i).name());
        }
    }

    // ── Mapping generation ─────────────────────────────────────────────

    public void testClickbench100mTargetMappingHas45Fields() {
        // The target index mapping has exactly 45 fields: 5 keys + 40 aggregates
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_100M;
        assertEquals("target mapping field count = ship field count = 45", 45, spec.shipFields().size());
    }

    // ── Hash stability ────────────────────────────────────────────────

    public void testClickbench100mHashStability() {
        // hashCode is stable across calls
        int h1 = MVDefinitionSpec.CLICKBENCH_100M.hashCode();
        int h2 = MVDefinitionSpec.CLICKBENCH_100M.hashCode();
        assertEquals(h1, h2);
    }

    public void testClickbench100mSourceDiffersFromFold() {
        assertNotEquals(
            "source and fold must have different SQL",
            MVDefinitionSpec.CLICKBENCH_100M.sql(),
            MVDefinitionSpec.CLICKBENCH_100M_FOLD.sql()
        );
    }

    // ── Error on unknown definitions ──────────────────────────────────

    public void testUnknownSourceThrows() {
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionSpec.source("nonexistent"));
    }

    public void testUnknownFoldThrows() {
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionSpec.fold("nonexistent"));
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
}
