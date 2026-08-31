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
 * Tests for {@link AggregateSpec} factory methods, SQL fragment generation,
 * state column decomposition, and edge cases.
 */
public class AggregateSpecTests extends OpenSearchTestCase {

    // ── COUNT factory ─────────────────────────────────────────────────────

    public void testCountFactory() {
        AggregateSpec count = AggregateSpec.count("cnt");

        assertEquals(AggregateSpec.AggFunction.COUNT, count.function());
        assertNull(count.sourceField());
        assertEquals("cnt", count.userAlias());
        assertEquals("long", count.targetMappingType());
        assertEquals(1, count.stateColumns().size());
        assertEquals("cnt", count.stateColumns().get(0).name());
        assertEquals("long", count.stateColumns().get(0).physicalType());
    }

    public void testCountPartialSql() {
        assertEquals("COUNT(*)", AggregateSpec.count("cnt").partialSqlFragment());
    }

    public void testCountFoldSql() {
        assertEquals("SUM(\"cnt\")", AggregateSpec.count("cnt").foldSqlFragment());
    }

    // ── SUM factory ───────────────────────────────────────────────────────

    public void testSumFactory() {
        AggregateSpec sum = AggregateSpec.sum("revenue", "total_revenue");

        assertEquals(AggregateSpec.AggFunction.SUM, sum.function());
        assertEquals("revenue", sum.sourceField());
        assertEquals("total_revenue", sum.userAlias());
        assertEquals("long", sum.targetMappingType());
        assertEquals(1, sum.stateColumns().size());
        assertEquals("total_revenue", sum.stateColumns().get(0).name());
    }

    public void testSumPartialSql() {
        assertEquals("SUM(\"price\")", AggregateSpec.sum("price", "sum_price").partialSqlFragment());
    }

    public void testSumFoldSql() {
        assertEquals("SUM(\"sum_price\")", AggregateSpec.sum("price", "sum_price").foldSqlFragment());
    }

    public void testSumNullFieldThrows() {
        expectThrows(NullPointerException.class, () -> AggregateSpec.sum(null, "alias"));
    }

    // ── MIN factory ───────────────────────────────────────────────────────

    public void testMinFactory() {
        AggregateSpec min = AggregateSpec.min("latency", "min_latency");

        assertEquals(AggregateSpec.AggFunction.MIN, min.function());
        assertEquals("latency", min.sourceField());
        assertEquals("min_latency", min.userAlias());
        assertEquals("long", min.targetMappingType());
        assertEquals("MIN(\"latency\")", min.partialSqlFragment());
        assertEquals("MIN(\"min_latency\")", min.foldSqlFragment());
    }

    public void testMinNullFieldThrows() {
        expectThrows(NullPointerException.class, () -> AggregateSpec.min(null, "alias"));
    }

    // ── MAX factory ───────────────────────────────────────────────────────

    public void testMaxFactory() {
        AggregateSpec max = AggregateSpec.max("latency", "max_latency");

        assertEquals(AggregateSpec.AggFunction.MAX, max.function());
        assertEquals("latency", max.sourceField());
        assertEquals("max_latency", max.userAlias());
        assertEquals("long", max.targetMappingType());
        assertEquals("MAX(\"latency\")", max.partialSqlFragment());
        assertEquals("MAX(\"max_latency\")", max.foldSqlFragment());
    }

    public void testMaxNullFieldThrows() {
        expectThrows(NullPointerException.class, () -> AggregateSpec.max(null, "alias"));
    }

    // ── AVG factory ───────────────────────────────────────────────────────

    public void testAvgFactory() {
        AggregateSpec avg = AggregateSpec.avg("Price");

        assertEquals(AggregateSpec.AggFunction.AVG, avg.function());
        assertEquals("Price", avg.sourceField());
        assertEquals("avg_Price", avg.userAlias());
        assertEquals("double", avg.targetMappingType());
    }

    public void testAvgDecomposesIntoTwoStateColumns() {
        AggregateSpec avg = AggregateSpec.avg("Price");

        assertEquals(2, avg.stateColumns().size());
        assertEquals("avg_count_Price", avg.stateColumns().get(0).name());
        assertEquals("long", avg.stateColumns().get(0).physicalType());
        assertEquals("avg_sum_Price", avg.stateColumns().get(1).name());
        assertEquals("long", avg.stateColumns().get(1).physicalType());
    }

    public void testAvgPartialSqlContainsCountAndSum() {
        AggregateSpec avg = AggregateSpec.avg("Metric");
        assertEquals("COUNT(\"Metric\"), SUM(\"Metric\")", avg.partialSqlFragment());
    }

    public void testAvgFoldSqlSumsStateColumns() {
        AggregateSpec avg = AggregateSpec.avg("Metric");
        assertEquals("SUM(\"avg_count_Metric\"), SUM(\"avg_sum_Metric\")", avg.foldSqlFragment());
    }

    public void testAvgNullFieldThrows() {
        expectThrows(NullPointerException.class, () -> AggregateSpec.avg(null));
    }

    // ── State columns immutability ────────────────────────────────────────

    public void testStateColumnsAreImmutable() {
        AggregateSpec count = AggregateSpec.count("cnt");
        expectThrows(UnsupportedOperationException.class, () -> count.stateColumns().add(new AggregateSpec.StateColumn("x", "long")));
    }

    // ── Record null validation ────────────────────────────────────────────

    public void testRecordRejectsNullFunction() {
        expectThrows(
            NullPointerException.class,
            () -> new AggregateSpec(
                null,
                "field",
                "alias",
                List.of(new AggregateSpec.StateColumn("col", "long")),
                "COUNT(*)",
                "SUM(\"cnt\")",
                "long"
            )
        );
    }

    public void testRecordRejectsNullAlias() {
        expectThrows(
            NullPointerException.class,
            () -> new AggregateSpec(
                AggregateSpec.AggFunction.COUNT,
                null,
                null,
                List.of(new AggregateSpec.StateColumn("col", "long")),
                "COUNT(*)",
                "SUM(\"cnt\")",
                "long"
            )
        );
    }

    public void testRecordRejectsEmptyStateColumns() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new AggregateSpec(AggregateSpec.AggFunction.COUNT, null, "cnt", List.of(), "COUNT(*)", "SUM(\"cnt\")", "long")
        );
    }

    // ── StateColumn record validation ─────────────────────────────────────

    public void testStateColumnRejectsNullName() {
        expectThrows(NullPointerException.class, () -> new AggregateSpec.StateColumn(null, "long"));
    }

    public void testStateColumnRejectsNullType() {
        expectThrows(NullPointerException.class, () -> new AggregateSpec.StateColumn("col", null));
    }

    // ── GroupKey tests ────────────────────────────────────────────────────

    public void testGroupKeyFactory() {
        GroupKey key = GroupKey.of("region", GroupKey.ColumnType.KEYWORD);
        assertEquals("region", key.name());
        assertEquals(GroupKey.ColumnType.KEYWORD, key.columnType());
        assertEquals("region", key.osFieldPath());
    }

    public void testGroupKeyOsType() {
        assertEquals("keyword", GroupKey.ColumnType.KEYWORD.osType());
        assertEquals("long", GroupKey.ColumnType.LONG.osType());
        assertEquals("integer", GroupKey.ColumnType.INTEGER.osType());
        assertEquals("double", GroupKey.ColumnType.DOUBLE.osType());
    }

    public void testGroupKeyRejectsNullName() {
        expectThrows(NullPointerException.class, () -> GroupKey.of(null, GroupKey.ColumnType.LONG));
    }

    public void testGroupKeyRejectsNullType() {
        expectThrows(NullPointerException.class, () -> GroupKey.of("field", null));
    }

    public void testGroupKeyCustomFieldPath() {
        GroupKey key = new GroupKey("region", GroupKey.ColumnType.KEYWORD, "metadata.region");
        assertEquals("region", key.name());
        assertEquals("metadata.region", key.osFieldPath());
    }

    // ── No DataFusion internals in any alias ──────────────────────────────

    public void testNoDataFusionInternalsInAnyFactoryAlias() {
        AggregateSpec[] specs = {
            AggregateSpec.count("cnt"),
            AggregateSpec.sum("x", "sum_x"),
            AggregateSpec.min("x", "min_x"),
            AggregateSpec.max("x", "max_x"),
            AggregateSpec.avg("x") };

        for (AggregateSpec spec : specs) {
            assertFalse(spec.userAlias() + " contains DataFusion internal", spec.userAlias().contains("Int64(1)"));
            assertFalse(spec.userAlias() + " contains DataFusion internal", spec.userAlias().contains("mv_input."));
            for (AggregateSpec.StateColumn sc : spec.stateColumns()) {
                assertFalse(sc.name() + " contains DataFusion internal", sc.name().contains("Int64(1)"));
                assertFalse(sc.name() + " contains DataFusion internal", sc.name().contains("mv_input."));
            }
        }
    }
}
