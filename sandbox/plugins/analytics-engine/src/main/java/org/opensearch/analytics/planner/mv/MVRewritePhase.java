/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.PlannerContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The MV transparent-rewrite phase: match + annotate, never restructure.
 *
 * <p>Runs in {@code PlannerImpl.runAllOptimizations} <b>after</b>
 * {@code decomposeAggregates} (so the tree and the stored MV definitions are
 * identically canonicalized: AVG decomposed, COUNT DISTINCT →
 * APPROX_COUNT_DISTINCT, CHECKED_LONG_SUM → SUM) and <b>before</b> {@code mark}
 * (so the marking phase, the Volcano split rule, and the
 * {@code DistributedAggregateRewriter} see the exact logical shape they see
 * today — the tree is returned untouched).
 *
 * <p>On a match, an {@link MVRewriteAnnotation} is recorded in the
 * {@link PlannerContext} side-channel keyed by table identity (decision D1);
 * the DAG layer attaches it to the shard fragment, and the shard exercises it
 * per segment based on snapshot coverage.
 *
 * <p>v0 match scope (deliberately narrow, never-wrong-only-slower):
 * <ul>
 *   <li>{@code LogicalAggregate} over an optional <i>trivial</i> Project
 *       (input refs only) over a {@code TableScan}. Any Filter, Join, or
 *       computed projection in between → no match.</li>
 *   <li>Simple group type only (no GROUPING SETS), no per-call DISTINCT or
 *       FILTER clauses.</li>
 *   <li>Group-by columns must equal the MV's group-by columns as a set;
 *       every query aggregate must appear verbatim (function + argument
 *       columns) in the MV definition. The MV may compute more aggregates
 *       than the query asks for.</li>
 *   <li>A table scanned more than once in the same plan (self-join) is never
 *       annotated — coverage binding is per-table-identity, so duplicate
 *       scans are conservatively skipped.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public final class MVRewritePhase {

    private static final Logger LOGGER = LogManager.getLogger(MVRewritePhase.class);

    private MVRewritePhase() {}

    /**
     * Scans {@code root} for MV-servable aggregate subtrees and records
     * annotations in {@code context}. Returns {@code root} unchanged.
     */
    public static RelNode annotate(RelNode root, PlannerContext context) {
        MVRegistry registry = context.getMVRegistry();
        if (registry == MVRegistry.EMPTY) {
            return root; // zero-cost path until MV metadata lands
        }
        List<LogicalAggregate> aggregates = new ArrayList<>();
        collectAggregates(root, aggregates);
        Set<String> seenTables = new HashSet<>();
        Set<String> duplicatedTables = new HashSet<>();
        collectScanKeys(root, seenTables, duplicatedTables);

        for (LogicalAggregate aggregate : aggregates) {
            matchAndAnnotate(aggregate, registry, context, duplicatedTables);
        }
        return root;
    }

    private static void collectAggregates(RelNode node, List<LogicalAggregate> out) {
        if (node instanceof LogicalAggregate aggregate) {
            out.add(aggregate);
        }
        for (RelNode input : node.getInputs()) {
            collectAggregates(input, out);
        }
    }

    private static void collectScanKeys(RelNode node, Set<String> seen, Set<String> duplicated) {
        if (node instanceof TableScan scan) {
            String key = tableKey(scan);
            if (seen.add(key) == false) {
                duplicated.add(key);
            }
        }
        for (RelNode input : node.getInputs()) {
            collectScanKeys(input, seen, duplicated);
        }
    }

    private static void matchAndAnnotate(
        LogicalAggregate aggregate,
        MVRegistry registry,
        PlannerContext context,
        Set<String> duplicatedTables
    ) {
        if (aggregate.getGroupType() != Aggregate.Group.SIMPLE) {
            return;
        }
        // Unwrap at most one trivial (input-refs-only) Project between the
        // aggregate and the scan, tracking the input-index remapping.
        RelNode input = aggregate.getInput();
        int[] fieldMapping = null;
        if (input instanceof Project project) {
            fieldMapping = trivialProjectMapping(project);
            if (fieldMapping == null) {
                return; // computed projection — out of v0 scope
            }
            input = project.getInput();
        }
        if ((input instanceof TableScan scan) == false) {
            return; // Filter/Join/anything else — out of v0 scope
        }
        TableScan scan = (TableScan) input;
        String key = tableKey(scan);
        if (duplicatedTables.contains(key)) {
            LOGGER.debug("mv-rewrite: table [{}] scanned more than once; skipping annotation", key);
            return;
        }

        List<String> scanFields = scan.getRowType().getFieldNames();
        Set<String> queryGroupBy = new HashSet<>();
        for (int bit : aggregate.getGroupSet()) {
            String column = resolveColumn(bit, fieldMapping, scanFields);
            if (column == null) {
                return;
            }
            queryGroupBy.add(column);
        }

        List<MVDefinition.AggregateSpec> queryAggs = new ArrayList<>();
        for (AggregateCall call : aggregate.getAggCallList()) {
            if (call.isDistinct() || call.filterArg >= 0) {
                return; // DISTINCT survived decompose, or FILTER clause — out of scope
            }
            List<String> argColumns = new ArrayList<>();
            for (int arg : call.getArgList()) {
                String column = resolveColumn(arg, fieldMapping, scanFields);
                if (column == null) {
                    return;
                }
                argColumns.add(column);
            }
            queryAggs.add(new MVDefinition.AggregateSpec(call.getAggregation().getName().toUpperCase(Locale.ROOT), argColumns));
        }

        String sourceIndex = scan.getTable().getQualifiedName().get(scan.getTable().getQualifiedName().size() - 1);
        for (MVDefinition definition : registry.eligibleFor(sourceIndex)) {
            if (matches(definition, queryGroupBy, queryAggs)) {
                context.putMVRewriteAnnotation(key, new MVRewriteAnnotation(definition.mvId(), definition.stateSchemaFingerprint()));
                LOGGER.debug("mv-rewrite: query aggregate over [{}] matched MV [{}]", sourceIndex, definition.mvId());
                return; // v0: first exact match wins
            }
        }
    }

    /** Group-by set-equality; every query aggregate present verbatim in the MV. */
    private static boolean matches(MVDefinition definition, Set<String> queryGroupBy, List<MVDefinition.AggregateSpec> queryAggs) {
        if (new HashSet<>(definition.groupByColumns()).equals(queryGroupBy) == false) {
            return false;
        }
        return new HashSet<>(definition.aggregates()).containsAll(queryAggs);
    }

    /**
     * Returns the project's output→input index mapping when every expression
     * is a bare {@link RexInputRef}; null otherwise.
     */
    private static int[] trivialProjectMapping(Project project) {
        List<RexNode> expressions = project.getProjects();
        int[] mapping = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            if ((expressions.get(i) instanceof RexInputRef ref) == false) {
                return null;
            }
            mapping[i] = ((RexInputRef) expressions.get(i)).getIndex();
        }
        return mapping;
    }

    private static String resolveColumn(int index, int[] fieldMapping, List<String> scanFields) {
        int scanIndex = fieldMapping == null ? index : (index < fieldMapping.length ? fieldMapping[index] : -1);
        return (scanIndex >= 0 && scanIndex < scanFields.size()) ? scanFields.get(scanIndex) : null;
    }

    /** Stable side-channel key for a scan: the dot-joined qualified table name. */
    public static String tableKey(TableScan scan) {
        return String.join(".", scan.getTable().getQualifiedName());
    }
}
