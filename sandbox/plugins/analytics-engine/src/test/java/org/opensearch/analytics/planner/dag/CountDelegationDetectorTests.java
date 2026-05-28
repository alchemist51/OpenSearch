/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.FilterTreeShape;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link CountDelegationDetector}.
 *
 * <p>Each test constructs a fragment shape manually and asserts whether the detector
 * upgrades CONJUNCTIVE → COUNT_DELEGATION or leaves the shape alone.
 */
public class CountDelegationDetectorTests extends BasePlannerRulesTests {

    private static final String DRIVING = "datafusion";
    private static final String ACCEPTING = "lucene";

    // ── Positive: should upgrade to COUNT_DELEGATION ─────────────────────

    public void testSingleDelegatedLeafWithCountStar() {
        // Aggregate(count(*)) → Filter(annotated_lucene) → scan
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.COUNT_DELEGATION, result);
    }

    public void testAndOfTwoDelegatedLeavesWithCountStar() {
        // Aggregate(count(*)) → Filter(AND(delegated, delegated)) → scan
        // Even though there are two Collector leaves, both are delegated and the filter
        // is a flat AND. Somesh's planner work fuses these into a single Lucene query
        // before reaching us; until then we still treat the shape as eligible because
        // the count_executor side rejects it (extract_single_count_collector requires one
        // leaf), keeping correctness while allowing the planner to upgrade speculatively.
        // For now the detector accepts it since runtime can still go bitmap if needed.
        // TODO: tighten once Somesh's fusion lands so this asserts exactly one Collector.
        RexNode and = rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            annotated(ACCEPTING),
            annotated(ACCEPTING)
        );
        OpenSearchFilter filter = buildFilter(and);
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.COUNT_DELEGATION, result);
    }

    public void testPerformanceDelegatedSingleLeaf() {
        // Mirrors the post-CBO shape of `BrowserCountry='us' | stats count()`:
        // a dual-viable predicate (lucene + datafusion) that the resolver narrowed onto
        // datafusion. viableBackends=[datafusion], performancePeers=[lucene] — the leaf
        // is performance-delegated and qualifies for the count fast path because Lucene
        // can answer the count metadata-only.
        OpenSearchFilter filter = buildFilter(performanceDelegated(DRIVING, ACCEPTING));
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.COUNT_DELEGATION, result);
    }

    public void testSingleAggregateMode() {
        // Aggregate(SINGLE, count(*)) — accepted at the data node when the planner
        // chose not to split into PARTIAL/FINAL (single-shard query, etc.).
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.SINGLE);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.COUNT_DELEGATION, result);
    }

    // ── Negative: must not upgrade ───────────────────────────────────────

    public void testRejectsNonConjunctiveInput() {
        // The detector only ever upgrades from CONJUNCTIVE — INTERLEAVED stays as-is.
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(
            aggregate, filter, FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, DRIVING
        );
        assertEquals(FilterTreeShape.INTERLEAVED_BOOLEAN_EXPRESSION, result);
    }

    public void testRejectsNoFilter() {
        // No filter at all (e.g. unfiltered count). Nothing to delegate, planner already
        // assigns NO_DELEGATION upstream — defensive null-safety.
        OpenSearchAggregate aggregate = countStarAggregate(scan(), AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, null, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsFilterWithNativePredicateMixed() {
        // Filter has a delegated AND a native predicate. The native predicate would still
        // need row decode to evaluate, so the count fast path can't help.
        RexNode and = rexBuilder.makeCall(
            SqlStdOperatorTable.AND,
            annotated(ACCEPTING),
            annotated(DRIVING)  // native — driving backend
        );
        OpenSearchFilter filter = buildFilter(and);
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(
            "Mixed lucene+native filter must stay CONJUNCTIVE so bitmap path handles it",
            FilterTreeShape.CONJUNCTIVE,
            result
        );
    }

    public void testRejectsNoAggregateAbove() {
        // Filter without an Aggregate above (e.g. WHERE ... | head 5 — sort/limit, not count).
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(filter, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsAggregateWithGroupBy() {
        // Aggregate(count(*) GROUP BY status) — group keys mean per-bucket counts that
        // require row data; the count fast path returns scalar totals only.
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        OpenSearchAggregate aggregate = new OpenSearchAggregate(
            cluster,
            RelTraitSet.createEmpty(),
            filter,
            ImmutableBitSet.of(0),  // non-empty group set
            null,
            List.of(countStarCall()),
            AggregateMode.PARTIAL,
            List.of(DRIVING),
            Map.of()
        );

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsCountOfColumn() {
        // Aggregate(count(status)) — count of a non-null column. Lucene's metadata fast
        // path returns docs-matching-the-filter; counting non-null values of a column
        // requires per-row inspection (column may have nulls).
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        AggregateCall countCol = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(0),  // count(col0) instead of count(*)
            -1,
            filter,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt_col"
        );
        OpenSearchAggregate aggregate = new OpenSearchAggregate(
            cluster,
            RelTraitSet.createEmpty(),
            filter,
            ImmutableBitSet.of(),
            null,
            List.of(countCol),
            AggregateMode.PARTIAL,
            List.of(DRIVING),
            Map.of()
        );

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsSumAggregate() {
        // Aggregate(SUM) instead of COUNT — different fast path. Build the SUM call locally
        // referencing column 0 of our 1-column filter input (the inherited sumCall() helper
        // assumes a 2-column scan and triggers a Calcite type-binding OOB here).
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        AggregateCall sumOnCol0 = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(0),
            -1,
            filter,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            "total"
        );
        OpenSearchAggregate aggregate = new OpenSearchAggregate(
            cluster,
            RelTraitSet.createEmpty(),
            filter,
            ImmutableBitSet.of(),
            null,
            List.of(sumOnCol0),
            AggregateMode.PARTIAL,
            List.of(DRIVING),
            Map.of()
        );

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsProjectionBetweenAggregateAndFilter() {
        // Aggregate → Project → Filter — the project means the planner expects column data
        // which the count fast path cannot deliver.
        OpenSearchFilter filter = buildFilter(annotated(ACCEPTING));
        OpenSearchProject project = new OpenSearchProject(
            cluster,
            RelTraitSet.createEmpty(),
            filter,
            List.of(rexBuilder.makeInputRef(filter, 0)),
            filter.getRowType(),
            List.of(DRIVING)
        );
        OpenSearchAggregate aggregate = countStarAggregate(project, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    public void testRejectsFilterWithOr() {
        // OR is not a flat conjunction — wouldn't be CONJUNCTIVE anyway, but defend
        // against the detector being called with an inconsistent shape.
        RexNode or = rexBuilder.makeCall(
            SqlStdOperatorTable.OR,
            annotated(ACCEPTING),
            annotated(ACCEPTING)
        );
        OpenSearchFilter filter = buildFilter(or);
        OpenSearchAggregate aggregate = countStarAggregate(filter, AggregateMode.PARTIAL);

        FilterTreeShape result = CountDelegationDetector.upgradeIfEligible(aggregate, filter, FilterTreeShape.CONJUNCTIVE, DRIVING);
        assertEquals(FilterTreeShape.CONJUNCTIVE, result);
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Produce a single-backend annotation (correctness-delegated when {@code backendId}
     * != driving). Mirrors a planner annotation that was always single-viable.
     */
    private AnnotatedPredicate annotated(String backendId) {
        RelDataType boolType = typeFactory.createJavaType(boolean.class);
        RexNode literal = rexBuilder.makeLiteral(true);
        return new AnnotatedPredicate(boolType, literal, List.of(backendId), 0);
    }

    /**
     * Produce a performance-delegated annotation: viable on multiple backends, then narrowed
     * to {@code drivingBackend} so the resolved annotation has {@code viableBackends=[driving]}
     * and {@code performanceDelegationBackends=[peer]}. Mirrors the post-CBO shape of a
     * dual-viable predicate like {@code BrowserCountry='us'} that the planner narrowed onto
     * datafusion while keeping lucene as the consult-able peer.
     */
    private AnnotatedPredicate performanceDelegated(String drivingBackend, String peerBackend) {
        RelDataType boolType = typeFactory.createJavaType(boolean.class);
        RexNode literal = rexBuilder.makeLiteral(true);
        AnnotatedPredicate dualViable = new AnnotatedPredicate(
            boolType, literal, List.of(peerBackend, drivingBackend), 0
        );
        return (AnnotatedPredicate) dualViable.narrowTo(drivingBackend);
    }

    private OpenSearchFilter buildFilter(RexNode condition) {
        return new OpenSearchFilter(
            cluster,
            RelTraitSet.createEmpty(),
            scan(),
            condition,
            List.of(DRIVING)
        );
    }

    private RelNode scan() {
        return stubScan(mockTable("test_index", "col"));
    }

    private OpenSearchAggregate countStarAggregate(RelNode input, AggregateMode mode) {
        return new OpenSearchAggregate(
            cluster,
            RelTraitSet.createEmpty(),
            input,
            ImmutableBitSet.of(),
            null,
            List.of(countStarCall()),
            mode,
            List.of(DRIVING),
            Map.of()
        );
    }
}
