/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link PerfPeerNarrowing} — verifies that the
 * {@link DropLucenePeerForNotEqualsEmptyStringRule} drops Lucene as a
 * performance-delegation peer for {@code col != ''} on the DataFusion-driver
 * stage alternative, while leaving non-empty {@code !=} predicates untouched
 * and not affecting the Lucene-driver alternative.
 */
public class PerfPeerNarrowingTests extends BasePlannerRulesTests {

    /** Keyword field with doc values duplicated in both parquet and lucene formats. */
    private Map<String, FieldStorageInfo> duplicatedKeywordFields() {
        return Map.of(
            "tag",
            new FieldStorageInfo(
                "tag",
                "keyword",
                FieldType.KEYWORD,
                List.of(MockDataFusionBackend.PARQUET_DATA_FORMAT, MockLuceneBackend.LUCENE_DATA_FORMAT),
                List.of(),
                List.of(),
                false
            )
        );
    }

    private RexNode makeNotEqualsLiteral(int fieldIndex, String literal) {
        return rexBuilder.makeCall(
            SqlStdOperatorTable.NOT_EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldIndex),
            rexBuilder.makeLiteral(literal)
        );
    }

    private QueryDAG buildAndFork(RelNode logicalPlan) {
        PlannerContext context = buildContextWithExplicitStorage(1, duplicatedKeywordFields(), List.of(DATAFUSION, LUCENE));
        RelNode optimized = runPlanner(logicalPlan, context);
        QueryDAG dag = DAGBuilder.build(optimized, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        return dag;
    }

    private org.apache.calcite.plan.RelOptTable keywordTable() {
        return mockTable("test_index", new String[] { "tag" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
    }

    /** Returns the AnnotatedPredicate inside the StagePlan's resolved filter, or null. */
    private static AnnotatedPredicate findPredicate(StagePlan plan) {
        RelNode current = plan.resolvedFragment();
        while (current != null && current instanceof OpenSearchFilter == false) {
            if (current.getInputs().isEmpty()) return null;
            current = current.getInputs().getFirst();
        }
        if (!(current instanceof OpenSearchFilter filter)) return null;
        List<AnnotatedPredicate> annotations = new ArrayList<>();
        collect(filter.getCondition(), annotations);
        return annotations.isEmpty() ? null : annotations.getFirst();
    }

    private static void collect(RexNode node, List<AnnotatedPredicate> out) {
        if (node instanceof AnnotatedPredicate ap) {
            out.add(ap);
            return;
        }
        if (node instanceof org.apache.calcite.rex.RexCall call) {
            for (RexNode operand : call.getOperands()) {
                collect(operand, out);
            }
        }
    }

    public void testDropsLucenePeerOnDfDriverForNotEqualsEmptyString() {
        QueryDAG dag = buildAndFork(LogicalFilter.create(stubScan(keywordTable()), makeNotEqualsLiteral(0, "")));

        // Pre-narrow assertion — DF alternative exists with Lucene as a perf peer.
        StagePlan dfPlanBefore = pickPlan(dag.rootStage(), MockDataFusionBackend.NAME);
        AnnotatedPredicate dfPredicateBefore = findPredicate(dfPlanBefore);
        assertNotNull("expected an AnnotatedPredicate on DF alternative", dfPredicateBefore);
        assertEquals(
            "DF predicate should have Lucene as a perf peer pre-narrow",
            List.of(MockLuceneBackend.NAME),
            dfPredicateBefore.getPerformanceDelegationBackends()
        );

        // Run the narrowing pass with a test-local rule wired to the mock backend names.
        PerfPeerNarrowing.narrowAll(
            dag,
            List.of(new DropLucenePeerForNotEqualsEmptyStringRule(MockDataFusionBackend.NAME, MockLuceneBackend.NAME))
        );

        StagePlan dfPlanAfter = pickPlan(dag.rootStage(), MockDataFusionBackend.NAME);
        AnnotatedPredicate dfPredicateAfter = findPredicate(dfPlanAfter);
        assertNotNull(dfPredicateAfter);
        assertTrue(
            "DF predicate's perf peer should be dropped for != '' " + dfPredicateAfter.getPerformanceDelegationBackends(),
            dfPredicateAfter.getPerformanceDelegationBackends().isEmpty()
        );
        assertEquals(
            "viableBackends must remain narrowed to DF",
            List.of(MockDataFusionBackend.NAME),
            dfPredicateAfter.getViableBackends()
        );

        // If a Lucene-driver alternative exists (e.g. for count fast-path shapes), the rule
        // must not have rewritten its predicate — it only fires when driver is DF + peer is Lucene.
        StagePlan lucenePlanAfter = findPlan(dag.rootStage(), MockLuceneBackend.NAME);
        if (lucenePlanAfter != null) {
            AnnotatedPredicate lucenePredicateAfter = findPredicate(lucenePlanAfter);
            assertNotNull(lucenePredicateAfter);
            assertEquals(
                "Lucene-driver alternative must not be narrowed",
                List.of(MockLuceneBackend.NAME),
                lucenePredicateAfter.getViableBackends()
            );
        }
    }

    public void testKeepsLucenePeerForNonEmptyLiteral() {
        QueryDAG dag = buildAndFork(LogicalFilter.create(stubScan(keywordTable()), makeNotEqualsLiteral(0, "hello")));
        StagePlan dfPlanBefore = pickPlan(dag.rootStage(), MockDataFusionBackend.NAME);
        AnnotatedPredicate before = findPredicate(dfPlanBefore);
        assertEquals(List.of(MockLuceneBackend.NAME), before.getPerformanceDelegationBackends());

        PerfPeerNarrowing.narrowAll(
            dag,
            List.of(new DropLucenePeerForNotEqualsEmptyStringRule(MockDataFusionBackend.NAME, MockLuceneBackend.NAME))
        );

        StagePlan dfPlanAfter = pickPlan(dag.rootStage(), MockDataFusionBackend.NAME);
        AnnotatedPredicate after = findPredicate(dfPlanAfter);
        assertEquals(
            "non-empty literal must keep Lucene as a perf peer",
            List.of(MockLuceneBackend.NAME),
            after.getPerformanceDelegationBackends()
        );
    }

    private static StagePlan pickPlan(Stage stage, String backendId) {
        StagePlan plan = findPlan(stage, backendId);
        if (plan == null) throw new AssertionError("no plan alternative for backend " + backendId);
        return plan;
    }

    private static StagePlan findPlan(Stage stage, String backendId) {
        for (StagePlan plan : stage.getPlanAlternatives()) {
            if (plan.backendId().equals(backendId)) return plan;
        }
        return null;
    }
}
