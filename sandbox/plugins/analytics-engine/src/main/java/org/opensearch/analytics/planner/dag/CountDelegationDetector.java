/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.FilterTreeShape;

/**
 * Decides whether a fragment qualifies for the COUNT_DELEGATION fast path — a strictly
 * narrower variant of {@link FilterTreeShape#CONJUNCTIVE} where the data node can answer
 * each segment with a single {@code Weight.count(LeafReaderContext)} call instead of
 * materializing a doc-id bitset and then counting rows in DataFusion.
 *
 * <h2>Eligibility (all must hold)</h2>
 * <ol>
 *   <li>Filter shape is {@link FilterTreeShape#CONJUNCTIVE} (already derived upstream).</li>
 *   <li>Filter is a flat AND (or single-leaf) of <b>delegated</b> {@link AnnotatedPredicate}s
 *       only — no driving-backend predicates would still need decoded rows to evaluate.</li>
 *   <li>Above the {@link OpenSearchFilter}, before any table scan, there
 *       is an {@link OpenSearchAggregate} whose {@code groupSet} is empty and whose every
 *       {@code AggregateCall} is {@code count(*)} (kind {@code COUNT} with empty arg list).
 *       PARTIAL or SINGLE mode are both acceptable — at the data node we only ever see
 *       non-FINAL aggregates over a Filter.</li>
 *   <li>No {@link OpenSearchProject} between the Filter and the Aggregate. A projection
 *       implies the planner expects per-row column data which the count fast path cannot
 *       provide.</li>
 * </ol>
 *
 * <p>If any condition fails, the existing CONJUNCTIVE shape stays — the bitmap path handles
 * those queries correctly today, the count fast path just doesn't fire.
 *
 * @opensearch.internal
 */
final class CountDelegationDetector {

    private static final Logger LOGGER = LogManager.getLogger(CountDelegationDetector.class);

    private CountDelegationDetector() {}

    /**
     * Returns {@link FilterTreeShape#COUNT_DELEGATION} when the fragment qualifies, otherwise
     * returns {@code currentShape} unchanged. The caller invokes this AFTER
     * {@link FilterTreeShapeDeriver}, so we only consider upgrading from CONJUNCTIVE.
     *
     * @param fragment           the resolved fragment root (annotations still present)
     * @param filter             the OpenSearchFilter inside the fragment (may be null)
     * @param currentShape       the shape derived by {@link FilterTreeShapeDeriver}
     * @param drivingBackendId   the filter operator's resolved backend (annotated predicates
     *                           targeting this backend are NOT delegated; they evaluate
     *                           natively and would still need row decode)
     */
    static FilterTreeShape upgradeIfEligible(
        RelNode fragment,
        OpenSearchFilter filter,
        FilterTreeShape currentShape,
        String drivingBackendId
    ) {
        if (currentShape != FilterTreeShape.CONJUNCTIVE) {
            return currentShape;
        }
        if (filter == null) {
            LOGGER.info("[count-delegation] skip: no filter in fragment");
            return currentShape;
        }
        if (!filterIsAllDelegatedLeaves(filter.getCondition(), drivingBackendId)) {
            LOGGER.info(
                "[count-delegation] skip: filter has non-delegated predicates (driving={}). Falling back to bitmap path.",
                drivingBackendId
            );
            return currentShape;
        }
        OpenSearchAggregate aggregate = findCountStarAggregateAbove(fragment, filter);
        if (aggregate == null) {
            LOGGER.info("[count-delegation] skip: no count(*) aggregate above filter");
            return currentShape;
        }
        if (hasProjectBetween(fragment, aggregate, filter)) {
            LOGGER.info("[count-delegation] skip: projection between aggregate and filter");
            return currentShape;
        }
        LOGGER.info(
            "[count-delegation] UPGRADE → COUNT_DELEGATION (mode={}, drivingBackend={})",
            aggregate.getMode(),
            drivingBackendId
        );
        return FilterTreeShape.COUNT_DELEGATION;
    }

    /**
     * Walks the filter condition tree confirming every leaf is an {@link AnnotatedPredicate}
     * delegated AWAY from the driving backend, and every interior node is an AND. A single
     * delegated AnnotatedPredicate (no surrounding AND) is also acceptable. Anything else —
     * native predicates (annotated to the driving backend), OR, NOT, raw RexCalls without
     * annotations — disqualifies the filter. Native predicates would need row decode; OR/NOT
     * would have been classified INTERLEAVED upstream anyway, but defend.
     */
    private static boolean filterIsAllDelegatedLeaves(RexNode node, String drivingBackendId) {
        if (node instanceof AnnotatedPredicate predicate) {
            // Two flavors of delegation count toward "delegated", mirroring
            // FilterTreeShapeDeriver.walk:
            //  1. CORRECTNESS — viableBackends differs from the driving backend (the only
            //     backend that can evaluate is the peer).
            //  2. PERFORMANCE — driving backend can evaluate natively, but a peer was also
            //     viable at planning time. The resolver narrowed onto the driving backend
            //     (so getViableBackends() is now a singleton == driving), and the peer is
            //     stored in performanceDelegationBackends. For COUNT_DELEGATION this is the
            //     common case: a keyword equality is dual-viable (lucene + datafusion);
            //     resolver picks datafusion as driver, Lucene becomes the performance peer
            //     we want to consult for the count fast path.
            boolean isCorrectness = !predicate.getViableBackends().getFirst().equals(drivingBackendId);
            boolean isPerformance = !predicate.getPerformanceDelegationBackends().isEmpty();
            boolean delegated = isCorrectness || isPerformance;
            LOGGER.info(
                "[count-delegation] leaf check: viableBackends={}, performancePeers={}, driving={}, delegated={}",
                predicate.getViableBackends(),
                predicate.getPerformanceDelegationBackends(),
                drivingBackendId,
                delegated
            );
            return delegated;
        }
        if (node instanceof RexCall call && call.getKind() == SqlKind.AND) {
            for (RexNode operand : call.getOperands()) {
                if (!filterIsAllDelegatedLeaves(operand, drivingBackendId)) {
                    return false;
                }
            }
            return !call.getOperands().isEmpty();
        }
        LOGGER.info(
            "[count-delegation] leaf check: rejected non-AnnotatedPredicate non-AND node class={}, kind={}",
            node.getClass().getSimpleName(),
            (node instanceof RexCall c) ? c.getKind() : "n/a"
        );
        return false;
    }

    /**
     * Walks UP from the fragment root looking for an {@link OpenSearchAggregate} that sits
     * directly above the given filter and is a pure count(*) shape. Returns the aggregate
     * if found, else null.
     *
     * <p>Implementation: starts at the fragment root, descends through inputs until it hits
     * the filter. If the path between root and filter contains exactly one OpenSearchAggregate
     * (and that aggregate is count-eligible), returns it.
     */
    private static OpenSearchAggregate findCountStarAggregateAbove(RelNode fragment, OpenSearchFilter filter) {
        OpenSearchAggregate aggregate = RelNodeUtils.findNode(fragment, OpenSearchAggregate.class);
        if (aggregate == null) {
            return null;
        }
        if (!isAggregateBelowFilter(aggregate, filter)) {
            // Defensive: the aggregate must sit above the filter on the linear input chain.
            // findNode returns the first match in DFS order; for a normal fragment that
            // matches the OpenSearchAggregate at the top.
            return null;
        }
        if (!aggregate.getGroupSet().isEmpty()) {
            return null;
        }
        for (AggregateCall aggCall : aggregate.getAggCallList()) {
            if (aggCall.getAggregation().getKind() != SqlKind.COUNT) {
                return null;
            }
            // count(*) → empty arg list. count(col) → single arg; we treat count(col) as
            // ineligible because it depends on column nullability and would require row
            // decode to count non-null values. Future work can lift this if Lucene exposes
            // a per-segment non-null count for the column.
            if (!aggCall.getArgList().isEmpty()) {
                return null;
            }
            // count(*) must not be filtered or distinct — both require row-level inspection.
            if (aggCall.isDistinct() || aggCall.filterArg >= 0) {
                return null;
            }
        }
        // Mode: PARTIAL or SINGLE only. FINAL aggregates run at the coordinator after the
        // data node has already returned partial counts — by the time FINAL fires, the
        // data-node fragment has finished and our backend doesn't see this aggregate.
        AggregateMode mode = aggregate.getMode();
        return (mode == AggregateMode.PARTIAL || mode == AggregateMode.SINGLE) ? aggregate : null;
    }

    /**
     * Checks that {@code filter} is reachable from {@code aggregate}'s input chain. We don't
     * require strict adjacency — there may be ExchangeReducers, Sorts, etc. in between in
     * theory, but in the count-fast-path shape we expect a direct Aggregate→Filter→TableScan
     * chain. Any non-Filter/non-Aggregate/non-TableScan node on the path falls through to
     * the {@link #hasProjectBetween} check which catches projections specifically.
     */
    private static boolean isAggregateBelowFilter(OpenSearchAggregate aggregate, OpenSearchFilter filter) {
        RelNode current = aggregate;
        while (current != null) {
            if (current == filter) {
                return true;
            }
            if (current.getInputs().isEmpty()) {
                return false;
            }
            current = current.getInputs().getFirst();
        }
        return false;
    }

    /**
     * Returns true if there is an {@link OpenSearchProject} on the linear input chain between
     * the aggregate and the filter. A project means the planner is materializing column data
     * that the count fast path cannot deliver.
     */
    private static boolean hasProjectBetween(RelNode fragment, OpenSearchAggregate aggregate, OpenSearchFilter filter) {
        RelNode current = aggregate.getInputs().isEmpty() ? null : aggregate.getInputs().getFirst();
        while (current != null && current != filter) {
            if (current instanceof OpenSearchProject) {
                return true;
            }
            if (current.getInputs().isEmpty()) {
                return false;
            }
            current = current.getInputs().getFirst();
        }
        return false;
    }
}
