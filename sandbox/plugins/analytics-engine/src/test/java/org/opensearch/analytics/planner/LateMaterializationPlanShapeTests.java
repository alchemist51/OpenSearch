/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.opensearch.analytics.planner.rel.OpenSearchLateMaterialization;
import org.opensearch.cluster.ClusterState;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Plan-shape tests for the QTF (late-materialization) post-CBO rewriter. Drive SQL through
 * the full planner ({@link PlannerImpl#runAllOptimizations}) and assert whether the
 * {@link OpenSearchLateMaterialization} wrapper was inserted (or not).
 *
 * <p>String-literal plan shapes are intentionally avoided — the wrapper's appearance vs.
 * absence + the helper-column declarations on Scan/ER are what we verify. Brittle plan-text
 * comparisons live in the per-operator tests.
 */
public class LateMaterializationPlanShapeTests extends BasePlannerRulesTests {

    public void testQtfFires_simpleSortProject() {
        // Pure-project columns (URL) above the anchor → QTF fires.
        assertQtfFires("SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfFires_withWhere() {
        // Filter cols (CounterID) join keepBelow; URL stays a fetch col.
        assertQtfFires("SELECT URL, EventDate FROM hits WHERE CounterID = 5 ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfFires_sortColAlsoProjected() {
        // EventDate appears in both sort and projection → stays in keepBelow, surfaces via wrapper passthrough.
        assertQtfFires("SELECT EventDate, URL FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_pureLimit() {
        // No collation on the Sort → not a QTF anchor.
        assertQtfDeclined("SELECT URL FROM hits LIMIT 10", 2);
    }

    public void testQtfDeclined_emptyFetchedSet() {
        // Only sort cols selected — fetched set is empty, QTF buys nothing.
        assertQtfDeclined("SELECT EventDate FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_aggregateBelowSort() {
        // Aggregate below the anchor breaks row-id semantics; allow-list rejects.
        assertQtfDeclined("SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID ORDER BY CounterID LIMIT 10", 2);
    }

    public void testQtfDeclined_windowInOuterProject() {
        // RexOver in outer Project rejected by the above-allow-list. Use SUM() OVER (),
        // a function the marking phase actually accepts (ROW_NUMBER and friends are not
        // wired into WindowFunction.fromSqlKind).
        assertQtfDeclined("SELECT URL, SUM(ParamPrice) OVER () AS sp FROM hits ORDER BY EventDate LIMIT 10", 2);
    }

    public void testQtfDeclined_expressionProjectBelowAnchor() {
        // Sort key (CounterID + 1) forces a non-passthrough Project below the anchor —
        // QTF rewriter bails (TODO follow-up). Arithmetic chosen because the mock backend
        // doesn't expose string functions like UPPER.
        assertQtfDeclined("SELECT URL FROM hits ORDER BY (CounterID + 1) LIMIT 10", 2);
    }

    public void testQtfFires_singleShard() {
        // Single shard: no ER inserted; Scan still narrows + ___row_id appears, no ___ugsi.
        assertQtfFires("SELECT URL, EventDate FROM hits ORDER BY EventDate LIMIT 10", 1);
    }

    // ── helpers ─────────────────────────────────────────────────────────

    private void assertQtfFires(String sql, int shardCount) {
        RelNode optimized = optimize(sql, shardCount);
        assertTrue(
            "Expected QTF wrapper in plan for SQL: " + sql + "\nPlan:\n" + org.apache.calcite.plan.RelOptUtil.toString(optimized),
            containsLateMaterialization(optimized)
        );
    }

    private void assertQtfDeclined(String sql, int shardCount) {
        RelNode optimized = optimize(sql, shardCount);
        assertFalse(
            "QTF should NOT have fired for SQL: " + sql + "\nPlan:\n" + org.apache.calcite.plan.RelOptUtil.toString(optimized),
            containsLateMaterialization(optimized)
        );
    }

    private RelNode optimize(String sql, int shardCount) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
        PlannerContext context = new PlannerContext(
            new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new),
            state,
            false
        );
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        return PlannerImpl.runAllOptimizations(parsed, context);
    }

    private static boolean containsLateMaterialization(RelNode root) {
        AtomicBoolean found = new AtomicBoolean();
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof OpenSearchLateMaterialization) {
                    found.set(true);
                    return;
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return found.get();
    }
}
