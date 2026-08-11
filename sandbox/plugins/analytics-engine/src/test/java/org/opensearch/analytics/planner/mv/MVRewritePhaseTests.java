/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.mv.MVDefinition.AggregateSpec;

import java.util.List;
import java.util.Map;

/**
 * Tests for {@link MVRewritePhase}: match + annotate into the
 * {@code PlannerContext} side-channel, tree always returned untouched.
 */
public class MVRewritePhaseTests extends BasePlannerRulesTests {

    private static final String INDEX = "test_index";

    private static MVDefinition sumByStatus() {
        return new MVDefinition(
            "mv-sum-by-status",
            INDEX,
            List.of("status"),
            List.of(AggregateSpec.of("SUM", "size"), AggregateSpec.of("COUNT")),
            "fp-1"
        );
    }

    private PlannerContext contextWith(MVDefinition... definitions) {
        PlannerContext context = buildContext("parquet", intFields());
        context.setMVRegistry(MVRegistry.ofStatic(Map.of(INDEX, List.of(definitions))));
        return context;
    }

    private TableScan scan() {
        return stubScan(mockTable(INDEX, "status", "size"));
    }

    public void testExactMatchAnnotates() {
        TableScan scan = scan();
        // SELECT status, SUM(size) GROUP BY status — group bit 0 = status, sum arg 1 = size
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan));
        PlannerContext context = contextWith(sumByStatus());

        RelNode result = MVRewritePhase.annotate(aggregate, context);

        assertSame("tree must be returned untouched", aggregate, result);
        MVRewriteAnnotation annotation = context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan));
        assertNotNull("expected an annotation for the scanned table", annotation);
        assertEquals("mv-sum-by-status", annotation.mvId());
        assertEquals("fp-1", annotation.stateSchemaFingerprint());
    }

    public void testQueryAggregatesMayBeSubsetOfMV() {
        // MV computes SUM + COUNT; query asks only COUNT(*) — still a match.
        TableScan scan = scan();
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), countStarCall(scan));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertNotNull(context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testGroupByMismatchDoesNotAnnotate() {
        TableScan scan = scan();
        // GROUP BY size (bit 1) but the MV groups by status.
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(1), countStarCall(scan));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertNull(context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testAggregateNotInMVDoesNotAnnotate() {
        TableScan scan = scan();
        // Query wants APPROX_COUNT_DISTINCT(size); the MV has SUM/COUNT only.
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), approxCountDistinctCall(scan));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertNull(context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testFilterBetweenAggregateAndScanDoesNotAnnotate() {
        TableScan scan = scan();
        RelNode filtered = makeFilter(scan, makeEquals(1, SqlTypeName.INTEGER, 10));
        LogicalAggregate aggregate = makeAggregate(filtered, ImmutableBitSet.of(0), countStarCall(filtered));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertNull(context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testTrivialProjectIsUnwrapped() {
        TableScan scan = scan();
        // Project [size, status] (reordered input refs) between aggregate and scan.
        List<RexNode> refs = List.of(
            rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            rexBuilder.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)
        );
        LogicalProject project = LogicalProject.create(scan, List.of(), refs, List.of("size", "status"), java.util.Set.of());
        // GROUP BY project field 1 (= scan field 0 = status), COUNT(*)
        LogicalAggregate aggregate = makeAggregate(project, ImmutableBitSet.of(1), countStarCall(project));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertNotNull(context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testDistinctCallDoesNotAnnotate() {
        TableScan scan = scan();
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), countDistinctCall(scan));
        PlannerContext context = contextWith(
            new MVDefinition("mv-dc", INDEX, List.of("status"), List.of(AggregateSpec.of("COUNT", "size")), "fp-dc")
        );

        MVRewritePhase.annotate(aggregate, context);

        assertNull("DISTINCT surviving decompose must never match", context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)));
    }

    public void testSelfJoinSkipsAnnotation() {
        TableScan left = scan();
        TableScan right = scan();
        LogicalJoin join = LogicalJoin.create(left, right, List.of(), rexBuilder.makeLiteral(true), java.util.Set.of(), JoinRelType.INNER);
        LogicalAggregate aggregate = makeAggregate(join, ImmutableBitSet.of(0), countStarCall(join));
        PlannerContext context = contextWith(sumByStatus());

        MVRewritePhase.annotate(aggregate, context);

        assertTrue("self-joined table must not be annotated", context.getMVRewriteAnnotations().isEmpty());
    }

    public void testEmptyRegistryIsNoOp() {
        TableScan scan = scan();
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan));
        PlannerContext context = buildContext("parquet", intFields()); // registry defaults to EMPTY

        RelNode result = MVRewritePhase.annotate(aggregate, context);

        assertSame(aggregate, result);
        assertTrue(context.getMVRewriteAnnotations().isEmpty());
    }

    public void testNoMVForIndexDoesNotAnnotate() {
        TableScan scan = scan();
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan));
        PlannerContext context = buildContext("parquet", intFields());
        context.setMVRegistry(MVRegistry.ofStatic(Map.of("other_index", List.of(sumByStatus()))));

        MVRewritePhase.annotate(aggregate, context);

        assertTrue(context.getMVRewriteAnnotations().isEmpty());
    }

    public void testFirstMatchWins() {
        TableScan scan = scan();
        LogicalAggregate aggregate = makeAggregate(scan, ImmutableBitSet.of(0), sumCall(scan));
        MVDefinition second = new MVDefinition("mv-second", INDEX, List.of("status"), List.of(AggregateSpec.of("SUM", "size")), "fp-2");
        PlannerContext context = contextWith(sumByStatus(), second);

        MVRewritePhase.annotate(aggregate, context);

        assertEquals("mv-sum-by-status", context.getMVRewriteAnnotation(MVRewritePhase.tableKey(scan)).mvId());
    }
}
