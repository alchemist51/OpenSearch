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

/**
 * Stage 4: Tests for the FFI metadata surfaced by {@link MVCompiledDefinition}
 * for the ordering and aggregate accumulator contracts that cross the Java/Rust
 * FFI boundary during merge.
 */
public class MVCompiledDefinitionFFIMetadataTests extends OpenSearchTestCase {

    // ── Ordering FFI metadata ─────────────────────────────────────────────

    public void testOrderingFFIMetadataMatchesGroupByOrdering() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("k2", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVCompiledDefinition.OrderingFFIMetadata meta = def.orderingFFIMetadata();

        assertEquals(3, meta.length());
        assertArrayEquals(new int[] { 0, 1, 2 }, meta.fieldIndices());
        // All ASC (wire token 0)
        assertArrayEquals(new int[] { 0, 0, 0 }, meta.directionTokens());
        // All NULLS_FIRST (wire token 0)
        assertArrayEquals(new int[] { 0, 0, 0 }, meta.nullPlacementTokens());
    }

    public void testOrderingFFIMetadataSingleKey() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "x", null, null, null);

        MVCompiledDefinition.OrderingFFIMetadata meta = def.orderingFFIMetadata();

        assertEquals(1, meta.length());
        assertArrayEquals(new int[] { 0 }, meta.fieldIndices());
        assertArrayEquals(new int[] { 0 }, meta.directionTokens());
        assertArrayEquals(new int[] { 0 }, meta.nullPlacementTokens());
    }

    public void testOrderingFFIMetadataLadderL3() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL3();
        MVCompiledDefinition.OrderingFFIMetadata meta = def.orderingFFIMetadata();

        // L3 has 10 group keys
        assertEquals(10, meta.length());
        for (int i = 0; i < 10; i++) {
            assertEquals(i, meta.fieldIndices()[i]);
            assertEquals(0, meta.directionTokens()[i]);
            assertEquals(0, meta.nullPlacementTokens()[i]);
        }
    }

    // ── Aggregate FFI metadata ────────────────────────────────────────────

    public void testAggregateFFIMetadataCountSumMinMax() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("GroupCol", "sumF", "minF", "maxF", null);

        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();

        // COUNT(cnt) + SUM(sum_sumF) + MIN(min_minF) + MAX(max_maxF) = 4 state cols
        assertEquals(4, aggMeta.length());
        // COUNT → SUM-fold (0)
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[0]);
        assertEquals("cnt", aggMeta.stateColumnNames()[0]);
        // SUM → SUM-fold (0)
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[1]);
        assertEquals("sum_sumF", aggMeta.stateColumnNames()[1]);
        // MIN → MIN-fold (1)
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_MIN, aggMeta.accumulatorTypes()[2]);
        assertEquals("min_minF", aggMeta.stateColumnNames()[2]);
        // MAX → MAX-fold (2)
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_MAX, aggMeta.accumulatorTypes()[3]);
        assertEquals("max_maxF", aggMeta.stateColumnNames()[3]);
    }

    public void testAggregateFFIMetadataAvgDecomposesAsSumFold() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("GroupCol", null, null, null, "price");

        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();

        // COUNT(cnt) + AVG(avg_count_price, avg_sum_price) = 3 state cols
        assertEquals(3, aggMeta.length());
        assertEquals("cnt", aggMeta.stateColumnNames()[0]);
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[0]);
        // AVG decomposes into count+sum, both SUM-folded
        assertEquals("avg_count_price", aggMeta.stateColumnNames()[1]);
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[1]);
        assertEquals("avg_sum_price", aggMeta.stateColumnNames()[2]);
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[2]);
    }

    public void testAggregateFFIMetadataFullQuad() {
        // clickbench_5m_url uses the full SUM/MIN/MAX/COUNT quad per metric
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();

        // 10 metrics × 4 agg types = 40 state columns
        assertEquals(40, aggMeta.length());
        // Each quad is: SUM, MIN, MAX, COUNT(field)
        for (int i = 0; i < 10; i++) {
            int base = i * 4;
            assertEquals("metric " + i + " SUM should be ACC_SUM", MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[base]);
            assertEquals("metric " + i + " MIN should be ACC_MIN", MVCompiledDefinition.AggregateFFIMetadata.ACC_MIN, aggMeta.accumulatorTypes()[base + 1]);
            assertEquals("metric " + i + " MAX should be ACC_MAX", MVCompiledDefinition.AggregateFFIMetadata.ACC_MAX, aggMeta.accumulatorTypes()[base + 2]);
            assertEquals("metric " + i + " COUNT should be ACC_SUM", MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[base + 3]);
        }
    }

    public void testAggregateFFIMetadataAccumulatorTypeConstants() {
        // Pin the wire constants — these cross FFI and must never change.
        assertEquals(0, MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM);
        assertEquals(1, MVCompiledDefinition.AggregateFFIMetadata.ACC_MIN);
        assertEquals(2, MVCompiledDefinition.AggregateFFIMetadata.ACC_MAX);
    }

    public void testAggregateFFIMetadataCountField() {
        // COUNT(field) should also fold as SUM (like COUNT(*))
        AggregateSpec countField = AggregateSpec.countField("my_field", "my_cnt");
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k", GroupKey.ColumnType.LONG)),
            List.of(countField)
        );
        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();
        assertEquals(1, aggMeta.length());
        assertEquals(MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[0]);
        assertEquals("my_cnt", aggMeta.stateColumnNames()[0]);
    }

    // ── Combined ordering + aggregate metadata consistency ────────────────

    public void testMetadataCoversAllStateColumns() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL1();

        MVCompiledDefinition.OrderingFFIMetadata orderingMeta = def.orderingFFIMetadata();
        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();

        // Total state columns = group keys + aggregate state columns
        int totalProjection = def.projectionOrder().size();
        assertEquals(totalProjection, orderingMeta.length() + aggMeta.length());
    }

    // ── MergeFFIBundle ────────────────────────────────────────────────────

    public void testMergeFFIBundleContainsAllMetadata() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL1();
        MVCompiledDefinition.MergeFFIBundle bundle = def.mergeFFIBundle();

        assertNotNull(bundle.ordering());
        assertNotNull(bundle.aggregates());
        assertNotNull(bundle.orderingIdentity());
        assertEquals(def.orderingFFIMetadata().length(), bundle.ordering().length());
        assertEquals(def.aggregateFFIMetadata().length(), bundle.aggregates().length());
        assertEquals(def.groupByOrdering().orderingIdentity(), bundle.orderingIdentity());
        assertEquals(def.projectionOrder().size(), bundle.totalStateColumns());
    }

    public void testMergeFFIBundleTotalStateColumnsIsGroupKeysPlusAggregates() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("G", "s", "m", "x", "a");
        MVCompiledDefinition.MergeFFIBundle bundle = def.mergeFFIBundle();

        // 1 group key + COUNT(cnt) + SUM(sum_s) + MIN(min_m) + MAX(max_x)
        // + AVG decomposed into (avg_count_a, avg_sum_a) = 1 + 5 = 7 but wait...
        // The group key count is in ordering, agg state cols are in aggregates.
        // So: ordering.length=1, aggregates.length = 1(cnt) + 1(sum_s) + 1(min_m) + 1(max_x) + 2(avg) = 6
        assertEquals(1, bundle.ordering().length());
        assertEquals(6, bundle.aggregates().length());
        assertEquals(7, bundle.totalStateColumns());
    }

    // ── Fold ops mapping ─────────────────────────────────────────────────

    public void testFoldOpsCorrectlyMapMinMaxFromAggMetadata() {
        // Verify that the merge strategy's fold-op mapping from accumulator
        // types to the byte encoding used by the Rust merge engine is correct.
        // ACC_SUM(0) → fold(1), ACC_MIN(1) → fold(2), ACC_MAX(2) → fold(3)
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("G", "s", "m", "x", null);
        MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();

        // State columns: cnt(COUNT→ACC_SUM=0), sum_s(SUM→ACC_SUM=0),
        //                min_m(MIN→ACC_MIN=1), max_x(MAX→ACC_MAX=2)
        assertEquals(4, aggMeta.length());
        assertEquals(0, aggMeta.accumulatorTypes()[0]); // COUNT → ACC_SUM
        assertEquals(0, aggMeta.accumulatorTypes()[1]); // SUM → ACC_SUM
        assertEquals(1, aggMeta.accumulatorTypes()[2]); // MIN → ACC_MIN
        assertEquals(2, aggMeta.accumulatorTypes()[3]); // MAX → ACC_MAX

        // Verify fold-op byte values: accType + 1
        int numGroupKeys = 1;
        byte[] foldOps = new byte[numGroupKeys + aggMeta.length()];
        for (int i = 0; i < numGroupKeys; i++) {
            foldOps[i] = (byte) 0; // GROUP_KEY
        }
        for (int i = 0; i < aggMeta.length(); i++) {
            foldOps[numGroupKeys + i] = (byte) (aggMeta.accumulatorTypes()[i] + 1);
        }
        assertEquals(0, foldOps[0]); // GROUP_KEY
        assertEquals(1, foldOps[1]); // SUM fold
        assertEquals(1, foldOps[2]); // SUM fold
        assertEquals(2, foldOps[3]); // MIN fold
        assertEquals(3, foldOps[4]); // MAX fold
    }

    // ── MergeCallParams ──────────────────────────────────────────────────

    public void testMergeCallParamsOrderingArrays() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("v", "sum_v"))
        );

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // Ordering arrays: 2 group keys
        assertArrayEquals(new int[] { 0, 1 }, params.orderingIndices());
        assertTrue(params.orderingAsc()[0]);
        assertTrue(params.orderingAsc()[1]);
        assertTrue(params.orderingNullsFirst()[0]);
        assertTrue(params.orderingNullsFirst()[1]);
    }

    public void testMergeCallParamsFoldOps() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("G", "s", "m", "x", null);

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // 1 group key + 4 agg state cols = 5 fold ops
        assertEquals(5, params.foldOps().length);
        assertEquals(0, params.foldOps()[0]); // GROUP_KEY
        assertEquals(1, params.foldOps()[1]); // cnt (COUNT → SUM fold)
        assertEquals(1, params.foldOps()[2]); // sum_s (SUM → SUM fold)
        assertEquals(2, params.foldOps()[3]); // min_m (MIN → MIN fold)
        assertEquals(3, params.foldOps()[4]); // max_x (MAX → MAX fold)
    }

    public void testMergeCallParamsAggColumnNames() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("G", "s", "m", "x", null);

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // MergeCallParams now carries PHYSICAL DataFusion Partial-stage names,
        // not user-facing aliases, because the Rust merge validates column names
        // against the Arrow schema in the IPC state files.
        assertArrayEquals(
            new String[] {
                "count(*)[count]",          // COUNT(*) → count(*)[count]
                "sum(mv_input.s)[sum]",     // SUM(s) → sum(mv_input.s)[sum]
                "min(mv_input.m)[value]",   // MIN(m) → min(mv_input.m)[value]
                "max(mv_input.x)[value]"    // MAX(x) → max(mv_input.x)[value]
            },
            params.aggColumnNames()
        );
    }

    public void testMergeCallParamsOrderingIdentity() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("region", GroupKey.ColumnType.LONG),
                GroupKey.of("os", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
        assertEquals("0:region:0:0;1:os:0:0", params.orderingIdentity());
    }

    public void testMergeCallParamsMatchesMergeFFIBundle() {
        // Verify that MergeCallParams produces values consistent with MergeFFIBundle
        MVCompiledDefinition def = MVCompiledDefinition.heavyL1();

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
        MVCompiledDefinition.MergeFFIBundle bundle = def.mergeFFIBundle();

        assertEquals(bundle.orderingIdentity(), params.orderingIdentity());
        assertEquals(bundle.ordering().length(), params.orderingIndices().length);
        assertArrayEquals(bundle.ordering().fieldIndices(), params.orderingIndices());
        // MergeCallParams carries PHYSICAL DataFusion names, while
        // AggregateFFIMetadata carries logical user aliases. Both arrays
        // must have the same length (same aggregate state columns).
        assertEquals(bundle.aggregates().length(), params.aggColumnNames().length);
    }

    public void testMergeCallParamsAvgDecomposition() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("G", null, null, null, "price");

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // 1 group key + 3 agg state cols (cnt, avg_count_price, avg_sum_price) = 4 fold ops
        assertEquals(4, params.foldOps().length);
        assertEquals(0, params.foldOps()[0]); // GROUP_KEY
        assertEquals(1, params.foldOps()[1]); // cnt → SUM fold
        assertEquals(1, params.foldOps()[2]); // avg_count_price → SUM fold
        assertEquals(1, params.foldOps()[3]); // avg_sum_price → SUM fold
        // Physical DataFusion names for AVG decomposition:
        // COUNT(*) → count(*)[count], COUNT(field) → count(mv_input.field)[count],
        // SUM(field) → sum(mv_input.field)[sum]
        assertArrayEquals(
            new String[] {
                "count(*)[count]",
                "count(mv_input.price)[count]",
                "sum(mv_input.price)[sum]"
            },
            params.aggColumnNames()
        );
    }

    public void testMergeCallParamsLadderL3() {
        MVCompiledDefinition def = MVCompiledDefinition.heavyL3();

        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // 10 group keys + 30 metrics × 4 quad = 130 total fold ops
        assertEquals(10, params.orderingIndices().length);
        assertEquals(130, params.foldOps().length);
        // First 10 should be GROUP_KEY (0)
        for (int i = 0; i < 10; i++) {
            assertEquals(0, params.foldOps()[i]);
        }
        // Rest should be non-zero fold ops
        for (int i = 10; i < 130; i++) {
            assertTrue("fold op at " + i + " should be > 0", params.foldOps()[i] > 0);
        }
    }

    // ── MergeCallParams ↔ MergeFFIBundle cross-validation ────────────────

    public void testMergeCallParamsOrderingIdentityMatchesGroupByOrdering() {
        // Verify that the ordering identity in MergeCallParams exactly matches
        // the one computed by MVGroupByOrdering — this is the merge-time
        // validation contract that prevents schema-drifted inputs.
        for (MVCompiledDefinition def : List.of(
            MVCompiledDefinition.clickbench100m(),
            MVCompiledDefinition.heavyL1(),
            MVCompiledDefinition.heavyL2(),
            MVCompiledDefinition.heavyL3(),
            MVCompiledDefinition.clickbench5mUrl()
        )) {
            MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
            assertEquals(
                "ordering identity mismatch for " + def,
                def.groupByOrdering().orderingIdentity(),
                params.orderingIdentity()
            );
        }
    }

    // ── MVGroupByOrdering.FFIArrays ──────────────────────────────────────

    public void testFFIArraysConsistentWithOrderingFFIMetadata() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        MVGroupByOrdering.FFIArrays arrays = def.groupByOrdering().toFFIArrays();
        MVCompiledDefinition.OrderingFFIMetadata meta = def.orderingFFIMetadata();

        assertEquals(meta.length(), arrays.length());
        assertArrayEquals(meta.fieldIndices(), arrays.indices());
        assertArrayEquals(meta.directionTokens(), arrays.directions());
        assertArrayEquals(meta.nullPlacementTokens(), arrays.nulls());
    }

    // ── orderingIdentityHash ─────────────────────────────────────────────

    /**
     * orderingIdentityHash must be deterministic: same ordering always
     * produces the same hash value.
     */
    public void testOrderingIdentityHashDeterministic() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench100m();
        long h1 = def.groupByOrdering().orderingIdentityHash();
        long h2 = def.groupByOrdering().orderingIdentityHash();
        assertEquals("orderingIdentityHash must be deterministic", h1, h2);
    }

    /**
     * Different orderings must produce different hash values.
     */
    public void testOrderingIdentityHashDifferentOrderings() {
        MVGroupByOrdering o1 = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        MVGroupByOrdering o2 = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        assertNotEquals(
            "Different orderings must produce different hashes",
            o1.orderingIdentityHash(),
            o2.orderingIdentityHash()
        );
    }

    /**
     * orderingIdentityHash must be non-zero for any non-empty ordering.
     */
    public void testOrderingIdentityHashNonZero() {
        for (MVCompiledDefinition def : List.of(
            MVCompiledDefinition.clickbench100m(),
            MVCompiledDefinition.heavyL1(),
            MVCompiledDefinition.heavyL2(),
            MVCompiledDefinition.heavyL3(),
            MVCompiledDefinition.clickbench5mUrl()
        )) {
            long hash = def.groupByOrdering().orderingIdentityHash();
            assertNotEquals(
                "orderingIdentityHash must be non-zero for " + def,
                0L,
                hash
            );
        }
    }

    /**
     * Verify the FNV-1a 128→lower64 implementation produces known values
     * for simple inputs, ensuring cross-language consistency.
     */
    public void testStableFnv1a128Lower64KnownEmptyInput() {
        // Empty byte array — should return the FNV offset basis lower 64 bits
        long hash = MVGroupByOrdering.stableFnv1a128Lower64(new byte[0]);
        // FNV-128 offset basis lower 64: 0x62b821756295c58d
        assertEquals(0x62b821756295c58dL, hash);
    }

    /**
     * Verify that the single-key ordering (index=0, asc=true, nulls_first=true)
     * produces a stable non-zero hash.
     */
    public void testOrderingIdentityHashSingleKey() {
        MVGroupByOrdering ordering = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        long hash = ordering.orderingIdentityHash();
        assertNotEquals(0L, hash);

        // Call again — must be stable
        assertEquals(hash, ordering.orderingIdentityHash());
    }

    // ── Physical name derivation (compaction merge fix) ──────────────────

    /**
     * Regression: MergeCallParams.aggColumnNames must carry the PHYSICAL
     * DataFusion Partial-stage column names (e.g. "sum(mv_input.X)[sum]"),
     * NOT the user-facing logical aliases (e.g. "sum_X"). The Rust
     * merge_state_streams validates names against the Arrow IPC schema.
     *
     * <p>This test reproduces the exact failure observed in live compaction:
     * "aggregate column name mismatch at position 3: expected 'sum_AdvEngineID',
     * got 'sum(mv_input.AdvEngineID)[sum]'".</p>
     */
    public void testMergeCallParamsPhysicalNamesMatchDataFusionConvention() {
        // Use clickbench_5m_url — the exact definition running on the live node
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // 3 group keys + 40 agg state cols = 43 total
        assertEquals(43, params.foldOps().length);
        assertEquals(40, params.aggColumnNames().length);

        // First metric is AdvEngineID with SUM/MIN/MAX/COUNT quad
        assertEquals("sum(mv_input.AdvEngineID)[sum]", params.aggColumnNames()[0]);
        assertEquals("min(mv_input.AdvEngineID)[value]", params.aggColumnNames()[1]);
        assertEquals("max(mv_input.AdvEngineID)[value]", params.aggColumnNames()[2]);
        assertEquals("count(mv_input.AdvEngineID)[count]", params.aggColumnNames()[3]);

        // Second metric is ResolutionWidth
        assertEquals("sum(mv_input.ResolutionWidth)[sum]", params.aggColumnNames()[4]);
        assertEquals("min(mv_input.ResolutionWidth)[value]", params.aggColumnNames()[5]);
        assertEquals("max(mv_input.ResolutionWidth)[value]", params.aggColumnNames()[6]);
        assertEquals("count(mv_input.ResolutionWidth)[count]", params.aggColumnNames()[7]);
    }

    /**
     * Verify physical names for the clickbench_100m (legacy-style) definition
     * which uses COUNT(*) + SUM per field (no quad).
     */
    public void testMergeCallParamsPhysicalNamesLegacyDefinition() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // clickbench_100m: 5 group keys + 10 metrics × 4 quad = 45 total cols
        // First agg col is SUM(AdvEngineID) → physical: sum(mv_input.AdvEngineID)[sum]
        assertEquals("sum(mv_input.AdvEngineID)[sum]", params.aggColumnNames()[0]);
    }

    /**
     * Verify that COUNT(*) produces the physical name "count(*)[count]"
     * and COUNT(field) produces "count(mv_input.field)[count]".
     */
    public void testMergeCallParamsPhysicalNamesCountStarVsCountField() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k", GroupKey.ColumnType.LONG)),
            List.of(
                AggregateSpec.count("cnt"),                    // COUNT(*)
                AggregateSpec.countField("myField", "my_cnt")  // COUNT(myField)
            )
        );
        MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        assertEquals("count(*)[count]", params.aggColumnNames()[0]);
        assertEquals("count(mv_input.myField)[count]", params.aggColumnNames()[1]);
    }

    /**
     * Verify physical name count matches logical state column count — they
     * must always have the same length even though the names differ.
     */
    public void testMergeCallParamsPhysicalNameCountMatchesLogicalCount() {
        for (MVCompiledDefinition def : List.of(
            MVCompiledDefinition.clickbench100m(),
            MVCompiledDefinition.heavyL1(),
            MVCompiledDefinition.heavyL2(),
            MVCompiledDefinition.heavyL3(),
            MVCompiledDefinition.clickbench5mUrl()
        )) {
            MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
            MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();
            assertEquals(
                "physical and logical agg column count mismatch for " + def,
                aggMeta.length(),
                params.aggColumnNames().length
            );
        }
    }
}
