/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Regression tests for {@link MVGroupByOrdering}, the complete physical GROUP BY
 * ordering contract owned by {@link MVCompiledDefinition}.
 *
 * <p>These tests pin the invariant that MV state must be sorted by the
 * <em>full</em> group-key tuple, and make explicit the gap in the current native
 * build/merge paths, which sort/advertise only the leading key
 * ({@code sort_to_indices(concatenated.column(0), ...)} and
 * {@code schema.field(0)} in the Rust sources). Stage&nbsp;3/4 will close that
 * gap by consuming this contract.</p>
 */
public class MVGroupByOrderingTests extends OpenSearchTestCase {

    private static MVCompiledDefinition threeKeyDefinition() {
        // 3 group keys + 2 aggregates. Keys are k0,k1,k2; aggregates must NOT
        // participate in ordering.
        return MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.LONG),
                GroupKey.of("k2", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("metric", "sum_metric"))
        );
    }

    // ── The old column-0-only behavior is insufficient ────────────────────

    /**
     * With a 3-key definition, rows that share {@code key0} but differ in
     * {@code key1}/{@code key2} are indistinguishable under a column-0-only sort
     * (every pair compares equal), but are strictly ordered by the full
     * contract. This is the exact failure mode of the current
     * {@code column(0)}-only native sort.
     */
    public void testColumnZeroOnlySortIsInsufficientForMultiKey() {
        MVGroupByOrdering ordering = threeKeyDefinition().groupByOrdering();
        assertEquals("expected all three group keys in the ordering", 3, ordering.size());

        // Rows keyed by state-field index: [k0, k1, k2].
        long[] rowA = { 1, 2, 3 };
        long[] rowB = { 1, 1, 9 };
        long[] rowC = { 1, 2, 1 };
        List<long[]> rows = new ArrayList<>(List.of(rowA, rowB, rowC));

        // Column-0-only comparator: exactly what the current Rust build path does.
        int firstIdx = ordering.stateFieldIndices().get(0);
        Comparator<long[]> columnZeroOnly = Comparator.comparingLong(r -> r[firstIdx]);
        // All rows share k0 == 1 → every pairwise comparison is a tie.
        assertEquals(0, columnZeroOnly.compare(rowA, rowB));
        assertEquals(0, columnZeroOnly.compare(rowA, rowC));
        assertEquals(0, columnZeroOnly.compare(rowB, rowC));

        // Full-contract comparator: lexicographic over every ordering key.
        Comparator<long[]> full = comparatorFrom(ordering);
        assertTrue("k1=1 row must sort before k1=2 rows", full.compare(rowB, rowA) < 0);
        assertTrue("within k1=2, k2=1 must sort before k2=3", full.compare(rowC, rowA) < 0);

        rows.sort(full);
        // Expected total order: (1,1,9), (1,2,1), (1,2,3)
        assertArrayEquals(new long[] { 1, 1, 9 }, rows.get(0));
        assertArrayEquals(new long[] { 1, 2, 1 }, rows.get(1));
        assertArrayEquals(new long[] { 1, 2, 3 }, rows.get(2));

        // And prove the tie set under column-0-only is strictly larger than
        // under the full contract (i.e. the single-key sort loses information).
        assertTrue("full contract must reference more than the first column for this definition", ordering.stateFieldIndices().size() > 1);
    }

    private static Comparator<long[]> comparatorFrom(MVGroupByOrdering ordering) {
        Comparator<long[]> cmp = null;
        for (MVGroupByOrdering.Key key : ordering.keys()) {
            int idx = key.stateFieldIndex();
            // Contract mandates ASCENDING for every key in the current design.
            assertEquals(MVGroupByOrdering.Direction.ASCENDING, key.direction());
            Comparator<long[]> next = Comparator.comparingLong(r -> r[idx]);
            cmp = (cmp == null) ? next : cmp.thenComparing(next);
        }
        return cmp;
    }

    // ── ASC + NULLS FIRST metadata for every key ──────────────────────────

    public void testEveryKeyIsAscendingNullsFirst() {
        MVGroupByOrdering ordering = threeKeyDefinition().groupByOrdering();
        assertFalse(ordering.isEmpty());
        for (MVGroupByOrdering.Key key : ordering.keys()) {
            assertEquals(MVGroupByOrdering.Direction.ASCENDING, key.direction());
            assertEquals(MVGroupByOrdering.NullPlacement.NULLS_FIRST, key.nullPlacement());
            // Arrow SortOptions.nulls_first must be true (matches the historical
            // sort_to_indices(..., None, None) default the build path relies on).
            assertTrue(key.nullPlacement().nullsFirst());
        }
    }

    public void testSqlOrderByRenderingIsStableAndNullsFirst() {
        MVGroupByOrdering ordering = threeKeyDefinition().groupByOrdering();
        assertEquals("\"k0\" ASC NULLS FIRST, \"k1\" ASC NULLS FIRST, \"k2\" ASC NULLS FIRST", ordering.toSqlOrderBy());
    }

    public void testWireTokensAreStable() {
        // These integer tokens cross the Java/Rust FFI boundary; pin them so a
        // later change can't silently reinterpret direction/null placement.
        assertEquals(0, MVGroupByOrdering.Direction.ASCENDING.wireToken());
        assertEquals(0, MVGroupByOrdering.NullPlacement.NULLS_FIRST.wireToken());
        assertEquals(1, MVGroupByOrdering.NullPlacement.NULLS_LAST.wireToken());
        assertTrue(MVGroupByOrdering.NullPlacement.NULLS_FIRST.nullsFirst());
        assertFalse(MVGroupByOrdering.NullPlacement.NULLS_LAST.nullsFirst());
    }

    // ── zero / one / multiple group keys per existing invariants ──────────

    public void testZeroGroupKeysRejectedByCompilerContract() {
        // The compiler contract forbids zero group keys; assert rejection rather
        // than weakening the invariant to accommodate an empty ordering.
        expectThrows(IllegalArgumentException.class, () -> MVCompiledDefinition.of(List.of(), List.of(AggregateSpec.count("cnt"))));
    }

    public void testSingleGroupKeyOrdering() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("only", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        assertEquals(1, ordering.size());
        assertEquals(List.of("only"), ordering.columnNames());
        assertEquals(List.of(0), ordering.stateFieldIndices());
    }

    public void testMultipleGroupKeyOrderingMatchesKeyCount() {
        // clickbench_100m has 5 group keys; heavy_l3 has 10 (incl. keyword keys).
        assertEquals(5, MVCompiledDefinition.clickbench100m().groupByOrdering().size());
        assertEquals(10, MVCompiledDefinition.heavyL3().groupByOrdering().size());
    }

    // ── stable state-field ordering ───────────────────────────────────────

    public void testStateFieldIndicesAreContiguousFromZero() {
        MVGroupByOrdering ordering = MVCompiledDefinition.heavyL3().groupByOrdering();
        List<Integer> indices = ordering.stateFieldIndices();
        for (int i = 0; i < indices.size(); i++) {
            assertEquals("group key " + i + " must map to state-field index " + i, Integer.valueOf(i), indices.get(i));
        }
    }

    public void testOrderingColumnsArePrefixOfProjectionOrder() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL2();
        MVGroupByOrdering ordering = def.groupByOrdering();
        List<String> projection = def.projectionOrder();
        List<String> orderingCols = ordering.columnNames();
        // Ordering columns must be exactly the leading prefix of the state layout.
        assertEquals(orderingCols, projection.subList(0, orderingCols.size()));
        assertEquals(def.groupKeys().size(), orderingCols.size());
    }

    public void testExpressionKeyCarriesAliasAndExpression() {
        // clickbench_5m_url leads with a derived expression key (event_bucket).
        MVGroupByOrdering ordering = MVCompiledDefinition.clickbench5mUrl().groupByOrdering();
        MVGroupByOrdering.Key first = ordering.keys().get(0);
        assertEquals("event_bucket", first.column());
        assertEquals("CAST(\"EventTime\" AS BIGINT) / 300000", first.sqlExpression());
        assertEquals(0, first.stateFieldIndex());
        // The materialized ordering column is the alias, never the raw expression.
        assertEquals("event_bucket", ordering.columnNames().get(0));
    }

    // ── no aggregate-state fields in the ordering ─────────────────────────

    public void testAggregateStateFieldsAreNotInOrdering() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench100m();
        MVGroupByOrdering ordering = def.groupByOrdering();
        List<String> orderingCols = ordering.columnNames();

        // Ordering size equals group-key count, never includes aggregates.
        assertEquals(def.groupKeys().size(), ordering.size());

        for (AggregateSpec agg : def.aggregates()) {
            assertFalse("aggregate alias leaked into ordering: " + agg.userAlias(), orderingCols.contains(agg.userAlias()));
            for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                assertFalse("aggregate state column leaked into ordering: " + sc.name(), orderingCols.contains(sc.name()));
                // Its state-field index must be >= number of group keys.
                int stateIdx = def.projectionOrder().indexOf(sc.name());
                assertTrue(stateIdx >= def.groupKeys().size());
                assertFalse(ordering.stateFieldIndices().contains(stateIdx));
            }
        }
    }

    // ── determinism of equality / hash / toString ─────────────────────────

    public void testEqualsHashCodeDeterministic() {
        MVGroupByOrdering a = threeKeyDefinition().groupByOrdering();
        MVGroupByOrdering b = threeKeyDefinition().groupByOrdering();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.toString(), b.toString());

        MVGroupByOrdering different = MVCompiledDefinition.clickbench100m().groupByOrdering();
        assertNotEquals(a, different);
    }

    // ── Stage 4: toFFIArrays convenience ──────────────────────────────────

    public void testToFFIArraysMatchesKeyEnumeration() {
        MVGroupByOrdering ordering = threeKeyDefinition().groupByOrdering();
        MVGroupByOrdering.FFIArrays arrays = ordering.toFFIArrays();

        assertEquals(3, arrays.length());
        assertArrayEquals(new int[] { 0, 1, 2 }, arrays.indices());
        // All ASC (wire token 0)
        assertArrayEquals(new int[] { 0, 0, 0 }, arrays.directions());
        // All NULLS_FIRST (wire token 0)
        assertArrayEquals(new int[] { 0, 0, 0 }, arrays.nulls());
    }

    public void testToFFIArraysSingleKey() {
        MVGroupByOrdering ordering = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "x", null, null, null)
            .groupByOrdering();
        MVGroupByOrdering.FFIArrays arrays = ordering.toFFIArrays();

        assertEquals(1, arrays.length());
        assertArrayEquals(new int[] { 0 }, arrays.indices());
        assertArrayEquals(new int[] { 0 }, arrays.directions());
        assertArrayEquals(new int[] { 0 }, arrays.nulls());
    }

    public void testToFFIArraysConsistentWithOrderingFFIMetadata() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL3();
        MVGroupByOrdering.FFIArrays arrays = def.groupByOrdering().toFFIArrays();
        MVCompiledDefinition.OrderingFFIMetadata meta = def.orderingFFIMetadata();

        // Both derivation paths must produce identical arrays
        assertEquals(meta.length(), arrays.length());
        assertArrayEquals(meta.fieldIndices(), arrays.indices());
        assertArrayEquals(meta.directionTokens(), arrays.directions());
        assertArrayEquals(meta.nullPlacementTokens(), arrays.nulls());
    }
}
