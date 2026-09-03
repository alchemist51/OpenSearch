/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.CapabilityRegistry;
import org.opensearch.analytics.planner.FieldStorageResolver;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.PlannerImpl;
import org.opensearch.analytics.planner.ClickBench;
import org.opensearch.analytics.planner.SqlPlannerTestFixture;
import org.opensearch.analytics.planner.mv.MVShapeResult.AggToken;
import org.opensearch.analytics.planner.mv.MVShapeResult.ColumnType;
import org.opensearch.analytics.planner.mv.MVShapeResult.Reason;
import org.opensearch.cluster.ClusterState;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * Targeted unit tests for {@link MVShapeMatcher}.
 *
 * <p>Accepts and reason-code rejects are hand-built from Calcite {@code Logical*} nodes and fed
 * directly to {@link MVShapeMatcher#match(RelNode)} for deterministic field names / types / group
 * sets (no dependence on SQL-lowering quirks). A few SQL-driven smoke tests exercise the real
 * post-CBO lowering, including the distributed FINAL→PARTIAL descent.
 */
public class MVShapeMatcherTests extends BasePlannerRulesTests {

    // ─────────────────────────── Accepts ───────────────────────────

    /** PPL: {@code source=hits | stats count() by status}. */
    public void testAccept_singleKeyCount() {
        RelNode scan = stubScan(mockTable("hits", "status", "size")); // INTEGER, INTEGER
        LogicalAggregate agg = makeAggregate(scan, countStarCall(scan)); // group {0}, COUNT() AS cnt

        MVShapeResult r = MVShapeMatcher.match(agg);

        assertTrue(r.toString(), r.isMatched());
        assertEquals(1, r.groupKeys().size());
        MVShapeResult.GroupKey k0 = r.groupKeys().get(0);
        assertEquals("status", k0.name());
        assertEquals(ColumnType.INTEGER, k0.type());
        assertNull(k0.expression());
        assertNull(k0.sourceColumn());
        assertEquals(1, r.aggregates().size());
        assertEquals(AggToken.COUNT, r.aggregates().get(0).function());
        assertNull(r.aggregates().get(0).field());
        // Descriptor JSON carries the wire contract, and NOT a definition hash (compiler recomputes it).
        assertTrue(r.descriptorJson(), r.descriptorJson().contains("\"descriptor_version\":1"));
        assertFalse(r.descriptorJson(), r.descriptorJson().contains("definition_hash"));
    }

    /**
     * PPL: {@code source=hits | stats sum(AdvEngineID) as sum_adv, min(AdvEngineID) as min_adv,
     * max(AdvEngineID) as max_adv, count() as cnt, count(AdvEngineID) as cnt_adv
     * by span(EventTime, 300000) as event_bucket, URL, UserID}. Exercises a derived bucket key,
     * three column types, the full aggregate token set, and GROUP BY key-order preservation.
     */
    public void testAccept_threeKeyBucketExprAndAllAggTokens() {
        RelNode scan = stubScan(
            mockTable(
                "hits",
                new String[] { "EventTime", "URL", "UserID", "AdvEngineID" },
                new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.INTEGER }
            )
        );
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        // event_bucket = CAST("EventTime" AS BIGINT) / 300000
        RexNode cast = rexBuilder.makeCast(bigint, rexBuilder.makeInputRef(scan, 0));
        RexNode lit = rexBuilder.makeExactLiteral(BigDecimal.valueOf(300000), typeFactory.createSqlType(SqlTypeName.INTEGER));
        RexNode bucket = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, cast, lit);
        LogicalProject sourceProject = LogicalProject.create(
            scan,
            List.of(),
            List.of(bucket, rexBuilder.makeInputRef(scan, 1), rexBuilder.makeInputRef(scan, 2), rexBuilder.makeInputRef(scan, 3)),
            List.of("event_bucket", "URL", "UserID", "AdvEngineID")
        );
        LogicalAggregate agg = LogicalAggregate.create(
            sourceProject,
            List.of(),
            ImmutableBitSet.of(0, 1, 2),
            null,
            List.of(
                mkAgg(SqlStdOperatorTable.SUM, List.of(3), sourceProject, 3, "sum_adv"),
                mkAgg(SqlStdOperatorTable.MIN, List.of(3), sourceProject, 3, "min_adv"),
                mkAgg(SqlStdOperatorTable.MAX, List.of(3), sourceProject, 3, "max_adv"),
                mkAgg(SqlStdOperatorTable.COUNT, List.of(), sourceProject, 3, "cnt"),
                mkAgg(SqlStdOperatorTable.COUNT, List.of(3), sourceProject, 3, "cnt_adv")
            )
        );

        MVShapeResult r = MVShapeMatcher.match(agg, "ppl", null);
        assertTrue(r.toString(), r.isMatched());

        // Group keys in GROUP BY order.
        assertEquals(List.of("event_bucket", "URL", "UserID"), r.groupKeys().stream().map(MVShapeResult.GroupKey::name).toList());
        MVShapeResult.GroupKey bucketKey = r.groupKeys().get(0);
        assertEquals(ColumnType.LONG, bucketKey.type());
        assertEquals("CAST(\"EventTime\" AS BIGINT) / 300000", bucketKey.expression());
        assertEquals("EventTime", bucketKey.sourceColumn());
        assertEquals(ColumnType.KEYWORD, r.groupKeys().get(1).type());
        assertNull(r.groupKeys().get(1).expression());
        assertEquals(ColumnType.LONG, r.groupKeys().get(2).type());

        // Aggregates, in order, with the full token set.
        assertEquals(
            List.of(AggToken.SUM, AggToken.MIN, AggToken.MAX, AggToken.COUNT, AggToken.COUNT_FIELD),
            r.aggregates().stream().map(MVShapeResult.Aggregate::function).toList()
        );
        assertEquals("AdvEngineID", r.aggregates().get(0).field());
        assertNull("COUNT(*) carries no field", r.aggregates().get(3).field());
        assertEquals("AdvEngineID", r.aggregates().get(4).field());

        // Descriptor JSON shape.
        String json = r.descriptorJson();
        assertTrue(json, json.contains("\"source_language\":\"ppl\""));
        assertTrue(json, json.contains("\"column_type\":\"LONG\""));
        assertTrue(json, json.contains("\"function\":\"COUNT_FIELD\""));
        assertTrue(json, json.contains("\"expression\""));
    }

    /** PPL: {@code source=hits | stats avg(size) as avg_size by status} — AVG(SUM/COUNT) quotient. */
    public void testAccept_avgToken() {
        RelNode scan = stubScan(mockTable("hits", "status", "size")); // INTEGER, INTEGER
        LogicalAggregate agg = LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(mkAgg(SqlStdOperatorTable.SUM, List.of(1), scan, 1, "$f1"), mkAgg(SqlStdOperatorTable.COUNT, List.of(), scan, 1, "$f2"))
        );
        // Trailing project: [status = $0, avg_size = CAST(/($1, $2))]
        RexNode div = rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 2));
        RexNode castDiv = rexBuilder.makeCast(typeFactory.createSqlType(SqlTypeName.INTEGER), div);
        LogicalProject top = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 0), castDiv),
            List.of("status", "avg_size")
        );

        MVShapeResult r = MVShapeMatcher.match(top);
        assertTrue(r.toString(), r.isMatched());
        assertEquals(1, r.groupKeys().size());
        assertEquals("status", r.groupKeys().get(0).name());
        assertEquals(1, r.aggregates().size());
        assertEquals(AggToken.AVG, r.aggregates().get(0).function());
        assertEquals("size", r.aggregates().get(0).field());
        assertEquals("avg_size", r.aggregates().get(0).alias());
        assertTrue(r.descriptorJson(), r.descriptorJson().contains("\"function\":\"AVG\""));
    }

    // ─────────────────────────── Rejects (one per reason) ───────────────────────────

    public void testReject_join() {
        RelNode a = stubScan(mockTable("hits", "CounterID", "UserID"));
        RelNode b = stubScan(mockTable("dim", "CounterID", "X"));
        LogicalJoin join = LogicalJoin.create(a, b, List.of(), rexBuilder.makeLiteral(true), Set.of(), JoinRelType.INNER);
        assertRejected(MVShapeMatcher.match(join), Reason.JOIN);
    }

    public void testReject_sortOrLimit() {
        RelNode scan = stubScan(mockTable("hits", "status", "size"));
        RelNode agg = makeAggregate(scan, countStarCall(scan));
        RelNode sort = makeSort(agg, 10); // ORDER BY + LIMIT
        assertRejected(MVShapeMatcher.match(sort), Reason.SORT_OR_LIMIT);
    }

    public void testReject_filterWhere() {
        RelNode scan = stubScan(mockTable("hits", "status", "size"));
        RelNode filter = makeFilter(scan, makeEquals(0, SqlTypeName.INTEGER, 5));
        RelNode agg = makeAggregate(filter, countStarCall(filter));
        assertRejected(MVShapeMatcher.match(agg), Reason.FILTER_WHERE);
    }

    public void testReject_distinctAgg() {
        RelNode scan = stubScan(mockTable("hits", "status", "size"));
        RelNode agg = makeAggregate(scan, countDistinctCall(scan)); // COUNT(DISTINCT $1)
        assertRejected(MVShapeMatcher.match(agg), Reason.DISTINCT_AGG);
    }

    public void testReject_unsupportedAgg() {
        RelNode scan = stubScan(mockTable("hits", "status", "size"));
        AggregateCall stddev = mkAgg(SqlStdOperatorTable.STDDEV_POP, List.of(1), scan, 1, "sd");
        RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(stddev));
        MVShapeResult r = MVShapeMatcher.match(agg);
        assertRejected(r, Reason.UNSUPPORTED_AGG);
        assertTrue(r.message().orElse(""), r.message().orElse("").contains("STDDEV_POP"));
    }

    public void testReject_subqueryOrMultiTable() {
        RelNode a = stubScan(mockTable("hits", "a", "b"));
        RelNode b = stubScan(mockTable("hits2", "a", "b"));
        LogicalUnion union = LogicalUnion.create(List.of(a, b), true);
        assertRejected(MVShapeMatcher.match(union), Reason.SUBQUERY_OR_MULTI_TABLE);
    }

    public void testReject_nonDeterministicExpr() {
        RelNode scan = stubScan(mockTable("hits", "val"));
        RexNode rand = rexBuilder.makeCall(SqlStdOperatorTable.RAND); // non-deterministic key
        LogicalProject sp = LogicalProject.create(
            scan,
            List.of(),
            List.of(rand, rexBuilder.makeInputRef(scan, 0)),
            List.of("r", "val")
        );
        RelNode agg = makeAggregate(sp, countStarCall(sp)); // group {0} = r
        MVShapeResult res = MVShapeMatcher.match(agg);
        assertRejected(res, Reason.NON_DETERMINISTIC_EXPR);
        assertTrue(res.message().orElse(""), res.message().orElse("").contains("RAND"));
    }

    public void testReject_zeroGroupKeys() {
        RelNode scan = stubScan(mockTable("hits", "status", "size"));
        RelNode agg = makeAggregate(scan, ImmutableBitSet.of(), countStarCall(scan)); // global aggregate
        assertRejected(MVShapeMatcher.match(agg), Reason.ZERO_GROUP_KEYS);
    }

    public void testReject_unsupportedKeyExpr() {
        RelNode scan = stubScan(mockTable("hits", "val"));
        RexNode mod = rexBuilder.makeCall(
            SqlStdOperatorTable.MOD,
            rexBuilder.makeInputRef(scan, 0),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(5), typeFactory.createSqlType(SqlTypeName.INTEGER))
        );
        LogicalProject sp = LogicalProject.create(scan, List.of(), List.of(mod, rexBuilder.makeInputRef(scan, 0)), List.of("m", "val"));
        RelNode agg = makeAggregate(sp, countStarCall(sp)); // group {0} = m (MOD, not whitelisted)
        assertRejected(MVShapeMatcher.match(agg), Reason.UNSUPPORTED_KEY_EXPR);
    }

    public void testReject_unsupportedType() {
        RelNode scan = stubScan(
            mockTable("hits", new String[] { "d", "val" }, new SqlTypeName[] { SqlTypeName.BINARY, SqlTypeName.INTEGER })
        );
        RelNode agg = makeAggregate(scan, countStarCall(scan)); // group {0} = d (BINARY, unmapped)
        assertRejected(MVShapeMatcher.match(agg), Reason.UNSUPPORTED_TYPE);
    }

    /** DATE type is now accepted as TIMESTAMP for span/date_bin keys. */
    public void testAccept_dateTypeGroupKeyMapsToTimestamp() {
        RelNode scan = stubScan(
            mockTable("hits", new String[] { "d", "val" }, new SqlTypeName[] { SqlTypeName.DATE, SqlTypeName.INTEGER })
        );
        RelNode agg = makeAggregate(scan, countStarCall(scan)); // group {0} = d (DATE)
        MVShapeResult r = MVShapeMatcher.match(agg);
        assertTrue(r.toString(), r.isMatched());
        assertEquals(ColumnType.TIMESTAMP, r.groupKeys().get(0).type());
    }

    // ─────────────────────────── SQL-driven smoke tests ───────────────────────────

    /** {@code SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID} (1 shard). */
    public void testSqlSmoke_countByKeyMatches() {
        RelNode plan = optimize("SELECT CounterID, COUNT(*) AS c FROM hits GROUP BY CounterID", 1);
        MVShapeResult r = MVShapeMatcher.match(plan);
        assertTrue(r.toString(), r.isMatched());
        assertEquals(1, r.groupKeys().size());
        assertEquals("CounterID", r.groupKeys().get(0).name());
        assertEquals(ColumnType.INTEGER, r.groupKeys().get(0).type());
        assertEquals(1, r.aggregates().size());
        assertEquals(AggToken.COUNT, r.aggregates().get(0).function());
    }

    /** Multi-shard {@code SUM} descends through the FINAL→ExchangeReducer→PARTIAL split. */
    public void testSqlSmoke_multiShardSumDescendsToPartial() {
        RelNode plan = optimize("SELECT CounterID, SUM(ParamPrice) AS s FROM hits GROUP BY CounterID", 2);
        MVShapeResult r = MVShapeMatcher.match(plan);
        assertTrue(r.toString(), r.isMatched());
        assertEquals(1, r.groupKeys().size());
        assertEquals("CounterID", r.groupKeys().get(0).name());
        assertEquals(1, r.aggregates().size());
        assertEquals(AggToken.SUM, r.aggregates().get(0).function());
        assertEquals("ParamPrice", r.aggregates().get(0).field());
    }

    /** A window function is rejected before any other check that its enclosing plan might trip. */
    public void testSqlSmoke_windowRejected() {
        RelNode plan = optimize("SELECT URL, SUM(ParamPrice) OVER () AS sp FROM hits ORDER BY EventDate LIMIT 10", 2);
        assertRejected(MVShapeMatcher.match(plan), Reason.WINDOW);
    }

    // ─────────────────────────── Helpers ───────────────────────────

    private void assertRejected(MVShapeResult r, Reason expected) {
        assertFalse("expected REJECTED but was MATCHED: " + r, r.isMatched());
        assertEquals("rejection reason mismatch (" + r.message().orElse("") + ")", expected, r.reason().orElseThrow());
    }

    /** Long-form {@link AggregateCall#create} with an inferred (null) return type to satisfy {@code typeMatchesInferred}. */
    private AggregateCall mkAgg(SqlAggFunction op, List<Integer> args, RelNode input, int groupCount, String name) {
        return AggregateCall.create(op, false, false, false, List.of(), args, -1, null, RelCollations.EMPTY, groupCount, input, null, name);
    }

    private RelNode optimize(String sql, int shardCount) {
        ClusterState state = SqlPlannerTestFixture.clusterStateWith(ClickBench.INDEX, ClickBench.BASIC_FIELDS, "parquet", shardCount);
        PlannerContext context = new PlannerContext(new CapabilityRegistry(List.of(DATAFUSION, LUCENE), FieldStorageResolver::new), state, false);
        RelNode parsed = SqlPlannerTestFixture.parseSql(sql, state);
        return PlannerImpl.runAllOptimizations(parsed, context);
    }
}
