/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.List;

/**
 * POC(mv): one MV definition in the form the write path needs — the columns
 * to capture from the composite broadcast (in buffer/state order: group keys
 * first) and the Partial-stopped definition SQL the native writer maintains.
 *
 * <p>Two hardcoded instances exist until MV metadata lands:
 * <ul>
 *   <li>{@link #SOURCE} — the source index's definition over raw fields.</li>
 *   <li>{@link #TARGET_FOLD} — the separate-index TARGET's definition: the
 *       FOLD of the source's state schema. Shipped state rows arrive as
 *       documents; folding them (SUM of count/sum states, MIN/MAX of extrema
 *       states) is aggregation-of-state, so the target is simply a composite
 *       index whose derived format maintains folded per-segment state — the
 *       embedded-MV shape, applied to the MV index itself.</li>
 * </ul>
 *
 * @param columns    captured columns, group keys first (buffer/state order)
 * @param groupKeys  number of leading group-key columns
 * @param sql        definition SQL over table {@code mv_input}, maintained
 *                   Partial-stopped by the native writer
 * @param shipFields target-index document field names for the shipped state
 *                   rows, positional with the finalized state batch's columns
 *                   (group keys first, then one field per aggregate state)
 */
public record MVDefinitionSpec(List<Column> columns, int groupKeys, String sql, List<String> shipFields) {

    /** Column types the POC forward buffer supports. */
    public enum ColumnType {
        UTF8,
        INT64
    }

    /** One captured column: broadcast field name + buffer vector type. */
    public record Column(String name, ColumnType type) {
    }

    public MVDefinitionSpec {
        columns = List.copyOf(columns);
        shipFields = List.copyOf(shipFields);
    }

    /**
     * Named spec lookup (POC stand-in for MV metadata): sources resolve
     * {@code index.mv.definition}; targets resolve the same name's fold.
     */
    public static MVDefinitionSpec source(String name) {
        return switch (name) {
            case "payments" -> SOURCE;
            case "pull_count_sum" -> PULL_COUNT_SUM;
            case "pull_count_sum_userid" -> PULL_COUNT_SUM_USERID;
            case "clickbench_q9" -> CLICKBENCH_Q9;
            case "clickbench_q9_native" -> CLICKBENCH_Q9_NATIVE;
            default -> throw new IllegalArgumentException("unknown mv definition [" + name + "]");
        };
    }

    public static MVDefinitionSpec fold(String name) {
        return switch (name) {
            case "payments" -> TARGET_FOLD;
            case "pull_count_sum" -> PULL_COUNT_SUM_FOLD;
            case "pull_count_sum_userid" -> PULL_COUNT_SUM_USERID_FOLD;
            case "clickbench_q9" -> CLICKBENCH_Q9_FOLD;
            case "clickbench_q9_native" -> CLICKBENCH_Q9_NATIVE_FOLD;
            default -> throw new IllegalArgumentException("unknown mv fold definition [" + name + "]");
        };
    }

    /** The source index's definition (raw fields → state). */
    public static final MVDefinitionSpec SOURCE = new MVDefinitionSpec(
        List.of(new Column("service", ColumnType.UTF8), new Column("status", ColumnType.UTF8), new Column("latency_ms", ColumnType.INT64)),
        2,
        MVConstants.MV_SQL,
        List.of("service", "status", "cnt", "lat_sum", "lat_min", "lat_max")
    );

    /** The target index's definition: the FOLD of the shipped state schema. */
    public static final MVDefinitionSpec TARGET_FOLD = new MVDefinitionSpec(
        List.of(
            new Column("service", ColumnType.UTF8),
            new Column("status", ColumnType.UTF8),
            new Column("cnt", ColumnType.INT64),
            new Column("lat_sum", ColumnType.INT64),
            new Column("lat_min", ColumnType.INT64),
            new Column("lat_max", ColumnType.INT64)
        ),
        2,
        "SELECT service, status, SUM(cnt), SUM(lat_sum), MIN(lat_min), MAX(lat_max) FROM mv_input GROUP BY service, status",
        List.of("service", "status", "cnt", "lat_sum", "lat_min", "lat_max")
    );

    /** Pull-engine count/sum definition over the source's raw columns. */
    public static final MVDefinitionSpec PULL_COUNT_SUM = new MVDefinitionSpec(
        List.of(new Column("RegionID", ColumnType.INT64), new Column("AdvEngineID", ColumnType.INT64)),
        1,
        "SELECT \"RegionID\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "count(Int64(1))[count]", "sum(mv_input.AdvEngineID)[sum]")
    );

    /** Schema-closed fold for immutable {@link #PULL_COUNT_SUM} state files. */
    public static final MVDefinitionSpec PULL_COUNT_SUM_FOLD = new MVDefinitionSpec(
        List.of(
            new Column("RegionID", ColumnType.INT64),
            new Column("count(Int64(1))[count]", ColumnType.INT64),
            new Column("sum(mv_input.AdvEngineID)[sum]", ColumnType.INT64)
        ),
        1,
        "SELECT \"RegionID\", SUM(\"count(Int64(1))[count]\"), SUM(\"sum(mv_input.AdvEngineID)[sum]\") "
            + "FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "count(Int64(1))[count]", "sum(mv_input.AdvEngineID)[sum]")
    );

    /** High-cardinality pull definition: GROUP BY UserID (~17.7M groups on 100M ClickBench). */
    public static final MVDefinitionSpec PULL_COUNT_SUM_USERID = new MVDefinitionSpec(
        List.of(new Column("UserID", ColumnType.INT64), new Column("AdvEngineID", ColumnType.INT64)),
        1,
        "SELECT \"UserID\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY \"UserID\"",
        List.of("UserID", "count(Int64(1))[count]", "sum(mv_input.AdvEngineID)[sum]")
    );

    /** Schema-closed fold for immutable {@link #PULL_COUNT_SUM_USERID} state files. */
    public static final MVDefinitionSpec PULL_COUNT_SUM_USERID_FOLD = new MVDefinitionSpec(
        List.of(
            new Column("UserID", ColumnType.INT64),
            new Column("count(Int64(1))[count]", ColumnType.INT64),
            new Column("sum(mv_input.AdvEngineID)[sum]", ColumnType.INT64)
        ),
        1,
        "SELECT \"UserID\", SUM(\"count(Int64(1))[count]\"), SUM(\"sum(mv_input.AdvEngineID)[sum]\") "
            + "FROM mv_input GROUP BY \"UserID\"",
        List.of("UserID", "count(Int64(1))[count]", "sum(mv_input.AdvEngineID)[sum]")
    );

    /**
     * ClickBench q9's mergeable core over the {@code hits} mapping (KB:
     * clickbench-reference): integer group key, short metrics. AVG is NOT in
     * the definition — it decomposes: the read computes
     * {@code AVG(ResolutionWidth) = SUM-state / COUNT-state} exactly.
     * Reference query: {@code SELECT RegionID, SUM(AdvEngineID), COUNT(*),
     * AVG(ResolutionWidth) FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10}.
     */
    public static final MVDefinitionSpec CLICKBENCH_Q9 = new MVDefinitionSpec(
        List.of(
            new Column("RegionID", ColumnType.INT64),
            new Column("AdvEngineID", ColumnType.INT64),
            new Column("ResolutionWidth", ColumnType.INT64)
        ),
        1,
        "SELECT \"RegionID\", COUNT(*), SUM(\"AdvEngineID\"), SUM(\"ResolutionWidth\"), MIN(\"ResolutionWidth\"), MAX(\"ResolutionWidth\") "
            + "FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "cnt", "adv_sum", "res_sum", "res_min", "res_max")
    );

    /** Fold of {@link #CLICKBENCH_Q9}'s state schema on the target. */
    public static final MVDefinitionSpec CLICKBENCH_Q9_FOLD = new MVDefinitionSpec(
        List.of(
            new Column("RegionID", ColumnType.INT64),
            new Column("cnt", ColumnType.INT64),
            new Column("adv_sum", ColumnType.INT64),
            new Column("res_sum", ColumnType.INT64),
            new Column("res_min", ColumnType.INT64),
            new Column("res_max", ColumnType.INT64)
        ),
        1,
        "SELECT \"RegionID\", SUM(cnt), SUM(adv_sum), SUM(res_sum), MIN(res_min), MAX(res_max) FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "cnt", "adv_sum", "res_sum", "res_min", "res_max")
    );

    /**
     * ZERO-TRANSLATION q9 (native read validation): the definition IS the
     * query — AVG kept intact so the state file carries DataFusion's own avg
     * state pair [count, sum] in the query's exact partial column order:
     * [RegionID, sum(AdvEngineID)[sum], count(*)[count], avg[count], avg[sum]].
     * A strict read then serves these files AS the fragment's Partial output
     * and the coordinator Final merges + evaluates (divide-once) natively.
     */
    public static final MVDefinitionSpec CLICKBENCH_Q9_NATIVE = new MVDefinitionSpec(
        List.of(
            new Column("RegionID", ColumnType.INT64),
            new Column("AdvEngineID", ColumnType.INT64),
            new Column("ResolutionWidth", ColumnType.INT64)
        ),
        1,
        "SELECT \"RegionID\", SUM(\"AdvEngineID\"), COUNT(*), AVG(\"ResolutionWidth\") FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "adv_sum", "cnt", "avg_cnt", "avg_sum")
    );

    /**
     * Fold of {@link #CLICKBENCH_Q9_NATIVE}'s state on the target. The
     * UNSIGNED cast keeps the folded avg-count column bit-identical to
     * DataFusion's avg state type (UInt64) — the strict read compares
     * positional types EXACTLY and throws on any drift (crude by design
     * for the validation phase).
     */
    public static final MVDefinitionSpec CLICKBENCH_Q9_NATIVE_FOLD = new MVDefinitionSpec(
        List.of(
            new Column("RegionID", ColumnType.INT64),
            new Column("adv_sum", ColumnType.INT64),
            new Column("cnt", ColumnType.INT64),
            new Column("avg_cnt", ColumnType.INT64),
            new Column("avg_sum", ColumnType.INT64)
        ),
        1,
        "SELECT \"RegionID\", SUM(adv_sum), SUM(cnt), SUM(CAST(avg_cnt AS BIGINT UNSIGNED)), SUM(avg_sum) FROM mv_input GROUP BY \"RegionID\"",
        List.of("RegionID", "adv_sum", "cnt", "avg_cnt", "avg_sum")
    );
}
