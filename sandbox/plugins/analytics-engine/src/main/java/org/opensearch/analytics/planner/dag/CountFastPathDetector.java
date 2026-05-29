/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Decides whether a fragment is eligible for the count fast path — a planner-time hint that
 * lets the data node attempt {@code IndexSearcher.count(BooleanQuery)} via the accepting
 * backend's {@link org.opensearch.analytics.spi.FilterDelegationHandle} instead of running
 * the indexed/DataFusion engine.
 *
 * <h2>Eligibility (all must hold)</h2>
 * <ol>
 *   <li>An {@link OpenSearchAggregate} above the {@link OpenSearchFilter}, with an empty group
 *       set, mode {@code PARTIAL} or {@code SINGLE}, and every {@code AggregateCall} a
 *       non-distinct, non-filtered {@code count(*)} or {@code count(col)}.</li>
 *   <li>For {@code count(col)} calls, {@code col}'s {@link FieldStorageInfo} must include the
 *       accepting backend in {@code indexFormats} — i.e. the accepting backend actually
 *       indexes the column, so {@code FieldExistsQuery(col)} returns the correct doc set.</li>
 *   <li>No {@link OpenSearchProject} between the Aggregate and the Filter.</li>
 *   <li>Every leaf in the filter condition is an {@link AnnotatedPredicate}, so the combiner
 *       can produce a fully-delegated subtree (no native residual the driver still has to
 *       evaluate). Pre-fusion check: necessary but not sufficient — caller must verify the
 *       combiner emitted exactly one {@link org.opensearch.analytics.spi.DelegatedExpression}
 *       after conversion.</li>
 *   <li>All annotations target the same accepting backend (correctness {@code viableBackends.first}
 *       and performance {@code performanceDelegationBackends.first} converge).</li>
 * </ol>
 *
 * <p>The detector returns an {@link Eligibility} record carrying the accepting backend id and
 * the deduplicated list of {@code count(col)} existence fields, or {@link Eligibility#NOT_ELIGIBLE}
 * when any check fails.
 *
 * @opensearch.internal
 */
final class CountFastPathDetector {

    private static final Logger LOGGER = LogManager.getLogger(CountFastPathDetector.class);

    private CountFastPathDetector() {}

    /**
     * Result of the pre-conversion eligibility check.
     *
     * @param eligible            true iff every gate above passed
     * @param acceptingBackendId  backend id all annotations target (e.g. "lucene"); null when not eligible
     * @param existenceFields     ordered, deduplicated list of column names from {@code count(col)} calls;
     *                            empty when only {@code count(*)} appears or when not eligible
     */
    record Eligibility(boolean eligible, String acceptingBackendId, List<String> existenceFields) {
        static final Eligibility NOT_ELIGIBLE = new Eligibility(false, null, List.of());
    }

    /**
     * Pre-conversion check on the resolved fragment with annotations intact. The combiner has
     * not yet run — caller must verify post-conversion that the combiner emitted exactly one
     * {@code DelegatedExpression} before setting the {@code countQuery} flag on the wire.
     */
    static Eligibility preCheck(RelNode fragment, String drivingBackendId) {
        OpenSearchFilter filter = RelNodeUtils.findNode(fragment, OpenSearchFilter.class);
        if (filter == null) {
            LOGGER.debug("[count-fast-path] skip: no filter in fragment");
            return Eligibility.NOT_ELIGIBLE;
        }

        OpenSearchAggregate aggregate = RelNodeUtils.findNode(fragment, OpenSearchAggregate.class);
        if (aggregate == null) {
            LOGGER.debug("[count-fast-path] skip: no aggregate in fragment");
            return Eligibility.NOT_ELIGIBLE;
        }
        if (!isAggregateBelowFilter(aggregate, filter)) {
            LOGGER.debug("[count-fast-path] skip: aggregate not on filter's input chain");
            return Eligibility.NOT_ELIGIBLE;
        }
        if (hasProjectBetween(aggregate, filter)) {
            LOGGER.debug("[count-fast-path] skip: project between aggregate and filter");
            return Eligibility.NOT_ELIGIBLE;
        }
        if (!aggregate.getGroupSet().isEmpty()) {
            LOGGER.debug("[count-fast-path] skip: aggregate has group set");
            return Eligibility.NOT_ELIGIBLE;
        }
        AggregateMode mode = aggregate.getMode();
        if (mode != AggregateMode.PARTIAL && mode != AggregateMode.SINGLE) {
            LOGGER.debug("[count-fast-path] skip: aggregate mode is {}", mode);
            return Eligibility.NOT_ELIGIBLE;
        }

        // Verify every leaf of the filter is an AnnotatedPredicate AND collect the accepting backend(s).
        Set<String> acceptingBackends = new LinkedHashSet<>();
        if (!collectAcceptingBackends(filter.getCondition(), drivingBackendId, acceptingBackends)) {
            LOGGER.debug("[count-fast-path] skip: filter has non-AnnotatedPredicate leaf");
            return Eligibility.NOT_ELIGIBLE;
        }
        if (acceptingBackends.size() != 1) {
            LOGGER.debug("[count-fast-path] skip: multiple accepting backends {}", acceptingBackends);
            return Eligibility.NOT_ELIGIBLE;
        }
        String acceptingBackend = acceptingBackends.iterator().next();

        // Validate every aggregate call: count(*) or count(col), col must be indexed by the accepting backend.
        List<FieldStorageInfo> filterFsi = filter.getOutputFieldStorage();
        LinkedHashSet<String> existenceFields = new LinkedHashSet<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.getAggregation().getKind() != SqlKind.COUNT) {
                LOGGER.debug("[count-fast-path] skip: non-count aggregate call {}", call);
                return Eligibility.NOT_ELIGIBLE;
            }
            if (call.isDistinct() || call.filterArg >= 0) {
                LOGGER.debug("[count-fast-path] skip: distinct/filtered count");
                return Eligibility.NOT_ELIGIBLE;
            }
            if (call.getArgList().isEmpty()) {
                continue;  // count(*) — always OK
            }
            if (call.getArgList().size() != 1) {
                LOGGER.debug("[count-fast-path] skip: count with {} args", call.getArgList().size());
                return Eligibility.NOT_ELIGIBLE;
            }
            int slot = call.getArgList().get(0);
            if (slot < 0 || slot >= filterFsi.size()) {
                LOGGER.debug("[count-fast-path] skip: count arg slot {} out of range", slot);
                return Eligibility.NOT_ELIGIBLE;
            }
            FieldStorageInfo fsi = filterFsi.get(slot);
            if (!fsi.getIndexFormats().contains(acceptingBackend)) {
                LOGGER.debug(
                    "[count-fast-path] skip: count(col={}) but {} does not index this column (formats={})",
                    fsi.getFieldName(),
                    acceptingBackend,
                    fsi.getIndexFormats()
                );
                return Eligibility.NOT_ELIGIBLE;
            }
            existenceFields.add(fsi.getFieldName());
        }

        LOGGER.debug(
            "[count-fast-path] ELIGIBLE: acceptingBackend={}, existenceFields={}",
            acceptingBackend,
            existenceFields
        );
        return new Eligibility(true, acceptingBackend, new ArrayList<>(existenceFields));
    }

    /**
     * Walks the filter condition; populates {@code accepting} with every leaf annotation's
     * accepting backend (correctness or performance). Returns false if any non-AnnotatedPredicate
     * leaf is encountered (e.g. raw RexCall, RexInputRef, literal) — those are native residuals
     * that the combiner can't fuse.
     */
    private static boolean collectAcceptingBackends(RexNode node, String drivingBackend, Set<String> accepting) {
        if (node instanceof AnnotatedPredicate ap) {
            String firstViable = ap.getViableBackends().getFirst();
            if (!firstViable.equals(drivingBackend)) {
                // Correctness-delegated: the only viable backend is the peer.
                accepting.add(firstViable);
            } else if (!ap.getPerformanceDelegationBackends().isEmpty()) {
                // Performance-delegated: driver natively evaluable, peer optional.
                accepting.add(ap.getPerformanceDelegationBackends().getFirst());
            } else {
                // Pure native — not delegable at all.
                return false;
            }
            return true;
        }
        if (node instanceof RexCall call) {
            SqlKind kind = call.getKind();
            if (kind != SqlKind.AND && kind != SqlKind.OR && kind != SqlKind.NOT) {
                // Operator at non-leaf position — not part of the AnnotatedPredicate marker
                // wrapping; means a native RexCall escaped the marker phase. Reject.
                return false;
            }
            for (RexNode operand : call.getOperands()) {
                if (!collectAcceptingBackends(operand, drivingBackend, accepting)) {
                    return false;
                }
            }
            return !call.getOperands().isEmpty();
        }
        return false;
    }

    /** Returns true when {@code filter} sits on the linear input chain of {@code aggregate}. */
    private static boolean isAggregateBelowFilter(OpenSearchAggregate aggregate, OpenSearchFilter filter) {
        RelNode current = aggregate;
        while (current != null) {
            if (current == filter) return true;
            if (current.getInputs().isEmpty()) return false;
            current = current.getInputs().getFirst();
        }
        return false;
    }

    /** Returns true when an {@link OpenSearchProject} sits between {@code aggregate} and {@code filter}. */
    private static boolean hasProjectBetween(OpenSearchAggregate aggregate, OpenSearchFilter filter) {
        RelNode current = aggregate.getInputs().isEmpty() ? null : aggregate.getInputs().getFirst();
        while (current != null && current != filter) {
            if (current instanceof OpenSearchProject) return true;
            if (current.getInputs().isEmpty()) return false;
            current = current.getInputs().getFirst();
        }
        return false;
    }
}
