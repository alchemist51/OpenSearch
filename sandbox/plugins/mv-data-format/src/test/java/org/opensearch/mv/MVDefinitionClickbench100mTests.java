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
 * Tests for the {@code clickbench_100m} one-target catch-up benchmark definition:
 * registry lookup, 45-column/order contract, per-field COUNT semantics, fold
 * correctness, and filtered SQL generation.
 */
public class MVDefinitionClickbench100mTests extends OpenSearchTestCase {

    private static final String DEF_NAME = "clickbench_100m";
    private static final int EXPECTED_GROUP_KEYS = 5;
    private static final int EXPECTED_METRIC_FIELDS = 10;
    private static final int AGGS_PER_FIELD = 4; // SUM, MIN, MAX, COUNT
    private static final int EXPECTED_TOTAL_COLUMNS = EXPECTED_GROUP_KEYS + (AGGS_PER_FIELD * EXPECTED_METRIC_FIELDS); // 45

    private static final List<String> EXPECTED_GROUP_KEY_NAMES = List.of(
        "EventTime", "RegionID", "OS", "CounterID", "IsRefresh"
    );

    private static final List<String> EXPECTED_METRIC_NAMES = List.of(
        "AdvEngineID", "ResolutionWidth", "ResolutionHeight", "ResolutionDepth", "ClientIP",
        "RemoteIP", "ConnectTiming", "DNSTiming", "FetchTiming", "SendTiming"
    );

    // ────────── Registry ──────────

    public void testSourceRegistryResolvesClickbench100m() {
        MVDefinitionSpec spec = MVDefinitionSpec.source(DEF_NAME);
        assertNotNull(spec);
        assertSame(MVDefinitionSpec.CLICKBENCH_100M, spec);
    }

    public void testFoldRegistryResolvesClickbench100m() {
        MVDefinitionSpec fold = MVDefinitionSpec.fold(DEF_NAME);
        assertNotNull(fold);
        assertSame(MVDefinitionSpec.CLICKBENCH_100M_FOLD, fold);
    }

    public void testOldName100mvIsNotRegistered() {
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionSpec.source("clickbench_100mv"));
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionSpec.fold("clickbench_100mv"));
    }

    // ────────── Source: 45-column contract ──────────

    public void testSourceHas5GroupKeys() {
        assertEquals(EXPECTED_GROUP_KEYS, MVDefinitionSpec.CLICKBENCH_100M.groupKeys());
    }

    public void testSourceHas15CapturedColumns() {
        // 5 group keys + 10 metric fields captured from source
        assertEquals(15, MVDefinitionSpec.CLICKBENCH_100M.columns().size());
    }

    public void testSourceGroupKeyColumnsFirst() {
        List<MVDefinitionSpec.Column> cols = MVDefinitionSpec.CLICKBENCH_100M.columns();
        for (int i = 0; i < EXPECTED_GROUP_KEYS; i++) {
            assertEquals(EXPECTED_GROUP_KEY_NAMES.get(i), cols.get(i).name());
        }
    }

    public void testSourceMetricColumnsAfterKeys() {
        List<MVDefinitionSpec.Column> cols = MVDefinitionSpec.CLICKBENCH_100M.columns();
        for (int i = 0; i < EXPECTED_METRIC_NAMES.size(); i++) {
            assertEquals(EXPECTED_METRIC_NAMES.get(i), cols.get(EXPECTED_GROUP_KEYS + i).name());
        }
    }

    public void testSourceShipFieldsExactly45() {
        assertEquals(EXPECTED_TOTAL_COLUMNS, MVDefinitionSpec.CLICKBENCH_100M.shipFields().size());
    }

    public void testSourceShipFieldsStartWithGroupKeys() {
        List<String> ship = MVDefinitionSpec.CLICKBENCH_100M.shipFields();
        for (int i = 0; i < EXPECTED_GROUP_KEYS; i++) {
            assertEquals(EXPECTED_GROUP_KEY_NAMES.get(i), ship.get(i));
        }
    }

    public void testSourceShipFieldsAllUnique() {
        List<String> ship = MVDefinitionSpec.CLICKBENCH_100M.shipFields();
        Set<String> unique = new HashSet<>(ship);
        assertEquals("duplicate ship field names detected", ship.size(), unique.size());
    }

    /**
     * Per-field COUNT contract: each metric field must have a _cnt ship field,
     * NOT a single global "cnt" column.
     */
    public void testSourceHasPerFieldCountNotGlobalCount() {
        List<String> ship = MVDefinitionSpec.CLICKBENCH_100M.shipFields();
        assertFalse("must not have global 'cnt' field", ship.contains("cnt"));
        // Each metric's 4 aggs are in order: sum, min, max, cnt
        for (int i = 0; i < EXPECTED_METRIC_NAMES.size(); i++) {
            int base = EXPECTED_GROUP_KEYS + (i * AGGS_PER_FIELD);
            String prefix = ship.get(base).replace("_sum", "");
            assertTrue("ship field " + base + " must end with _sum", ship.get(base).endsWith("_sum"));
            assertTrue("ship field " + (base + 1) + " must end with _min", ship.get(base + 1).endsWith("_min"));
            assertTrue("ship field " + (base + 2) + " must end with _max", ship.get(base + 2).endsWith("_max"));
            assertTrue("ship field " + (base + 3) + " must end with _cnt", ship.get(base + 3).endsWith("_cnt"));
            assertEquals(prefix + "_sum", ship.get(base));
            assertEquals(prefix + "_min", ship.get(base + 1));
            assertEquals(prefix + "_max", ship.get(base + 2));
            assertEquals(prefix + "_cnt", ship.get(base + 3));
        }
    }

    // ────────── Source SQL: no global COUNT(*), per-field COUNT ──────────

    public void testSourceSqlHasNoGlobalCountStar() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M.sql();
        // COUNT(*) must not appear; only COUNT("FieldName") forms
        assertFalse("source SQL must not contain COUNT(*)", sql.contains("COUNT(*)"));
    }

    public void testSourceSqlHasCountPerMetricField() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M.sql();
        for (String field : EXPECTED_METRIC_NAMES) {
            assertTrue(
                "source SQL must contain COUNT(\"" + field + "\")",
                sql.contains("COUNT(\"" + field + "\")")
            );
        }
    }

    public void testSourceSqlHasSumMinMaxPerMetricField() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M.sql();
        for (String field : EXPECTED_METRIC_NAMES) {
            assertTrue("SUM missing for " + field, sql.contains("SUM(\"" + field + "\")"));
            assertTrue("MIN missing for " + field, sql.contains("MIN(\"" + field + "\")"));
            assertTrue("MAX missing for " + field, sql.contains("MAX(\"" + field + "\")"));
        }
    }

    public void testSourceSqlGroupByAll5Keys() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M.sql();
        assertTrue(sql.contains("GROUP BY \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\""));
    }

    // ────────── Fold: 45-column contract ──────────

    public void testFoldShipFieldsExactly45() {
        assertEquals(EXPECTED_TOTAL_COLUMNS, MVDefinitionSpec.CLICKBENCH_100M_FOLD.shipFields().size());
    }

    public void testFoldColumnsExactly45() {
        assertEquals(EXPECTED_TOTAL_COLUMNS, MVDefinitionSpec.CLICKBENCH_100M_FOLD.columns().size());
    }

    public void testFoldHas5GroupKeys() {
        assertEquals(EXPECTED_GROUP_KEYS, MVDefinitionSpec.CLICKBENCH_100M_FOLD.groupKeys());
    }

    public void testFoldShipFieldsMatchSource() {
        assertEquals(MVDefinitionSpec.CLICKBENCH_100M.shipFields(), MVDefinitionSpec.CLICKBENCH_100M_FOLD.shipFields());
    }

    /** Fold SQL: counts fold with SUM, mins with MIN, maxes with MAX. */
    public void testFoldSqlPreservesAggSemantics() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M_FOLD.sql();
        // Each metric prefix's _cnt fields are folded with SUM
        assertTrue(sql.contains("SUM(adv_cnt)"));
        assertTrue(sql.contains("SUM(resw_cnt)"));
        // min fields folded with MIN
        assertTrue(sql.contains("MIN(adv_min)"));
        assertTrue(sql.contains("MIN(resw_min)"));
        // max fields folded with MAX
        assertTrue(sql.contains("MAX(adv_max)"));
        assertTrue(sql.contains("MAX(resw_max)"));
        // sum fields folded with SUM
        assertTrue(sql.contains("SUM(adv_sum)"));
        assertTrue(sql.contains("SUM(resw_sum)"));
        // No global COUNT(*) in fold either
        assertFalse(sql.contains("COUNT(*)"));
    }

    // ────────── All columns are INT64 ──────────

    public void testAllSourceColumnsAreInt64() {
        for (MVDefinitionSpec.Column col : MVDefinitionSpec.CLICKBENCH_100M.columns()) {
            assertEquals(col.name() + " must be INT64", MVDefinitionSpec.ColumnType.INT64, col.type());
        }
    }

    public void testAllFoldColumnsAreInt64() {
        for (MVDefinitionSpec.Column col : MVDefinitionSpec.CLICKBENCH_100M_FOLD.columns()) {
            assertEquals(col.name() + " must be INT64", MVDefinitionSpec.ColumnType.INT64, col.type());
        }
    }

    // ────────── SELECT column order matches ship field order ──────────

    /**
     * The source SQL SELECT column order must match the ship field order:
     * 5 group keys, then for each metric: SUM, MIN, MAX, COUNT.
     */
    public void testSourceSqlSelectOrderMatchesShipFields() {
        String sql = MVDefinitionSpec.CLICKBENCH_100M.sql();
        String selectPart = sql.substring(sql.indexOf("SELECT ") + 7, sql.indexOf(" FROM "));
        String[] selectCols = selectPart.split(",\\s*");
        List<String> shipFields = MVDefinitionSpec.CLICKBENCH_100M.shipFields();
        // SELECT should have exactly 45 expressions matching 45 ship fields
        assertEquals("SELECT column count must match ship field count", shipFields.size(), selectCols.length);
    }
}
