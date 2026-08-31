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
            case "clickbench_100m" -> CLICKBENCH_100M;
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
            case "clickbench_100m" -> CLICKBENCH_100M_FOLD;
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

    // ────────────────────────────────────────────────────────────────────
    //  100M one-target catch-up benchmark definition
    //
    //  One source index (clickbench, 100M docs), one target MV index, one
    //  MVDefinitionSpec, one primary shard, one MVArtifactPoller.
    //
    //  GROUP BY: EventTime (raw), RegionID, OS, CounterID, IsRefresh
    //  Metrics : SUM / MIN / MAX / COUNT(field)  ×  10 integer fields
    //            = 40 metric columns  +  5 group keys  = 45 total
    //  No global COUNT(*) — each field carries its own per-field COUNT.
    //
    //  10 metric fields: AdvEngineID, ResolutionWidth, ResolutionHeight,
    //  ResolutionDepth, ClientIP, RemoteIP, ConnectTiming, DNSTiming,
    //  FetchTiming, SendTiming
    // ────────────────────────────────────────────────────────────────────

    /** Column names for the 10 metric fields used by the 100M benchmark. */
    private static final List<String> METRIC_100M = List.of(
        "AdvEngineID", "ResolutionWidth", "ResolutionHeight", "ResolutionDepth", "ClientIP",
        "RemoteIP", "ConnectTiming", "DNSTiming", "FetchTiming", "SendTiming"
    );

    /** Column names for the 5 group-by keys. */
    private static final List<String> GROUP_100M = List.of(
        "EventTime", "RegionID", "OS", "CounterID", "IsRefresh"
    );

    /**
     * 100M one-target catch-up benchmark: wide-column GROUP BY over
     * ClickBench 100M documents with 5 group keys and 4 per-field
     * aggregates (SUM/MIN/MAX/COUNT) across 10 integer fields = 45
     * columns per state row. One source, one target, one poller.
     *
     * <p>EventTime drives cardinality (~100M expected groups at raw
     * granularity). The single target MV stresses poller catch-up
     * throughput against wide-column aggregation, not concurrency.
     */
    public static final MVDefinitionSpec CLICKBENCH_100M = new MVDefinitionSpec(
        // Columns: 5 group keys + 10 metric fields captured from the source
        List.of(
            new Column("EventTime", ColumnType.INT64),
            new Column("RegionID", ColumnType.INT64),
            new Column("OS", ColumnType.INT64),
            new Column("CounterID", ColumnType.INT64),
            new Column("IsRefresh", ColumnType.INT64),
            new Column("AdvEngineID", ColumnType.INT64),
            new Column("ResolutionWidth", ColumnType.INT64),
            new Column("ResolutionHeight", ColumnType.INT64),
            new Column("ResolutionDepth", ColumnType.INT64),
            new Column("ClientIP", ColumnType.INT64),
            new Column("RemoteIP", ColumnType.INT64),
            new Column("ConnectTiming", ColumnType.INT64),
            new Column("DNSTiming", ColumnType.INT64),
            new Column("FetchTiming", ColumnType.INT64),
            new Column("SendTiming", ColumnType.INT64)
        ),
        5,  // 5 group keys
        // Source SQL: 5 group keys + (SUM + MIN + MAX + COUNT) × 10 fields = 45 output columns
        // No global COUNT(*); each field gets its own COUNT(field).
        "SELECT \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\", "
            + "SUM(\"AdvEngineID\"), MIN(\"AdvEngineID\"), MAX(\"AdvEngineID\"), COUNT(\"AdvEngineID\"), "
            + "SUM(\"ResolutionWidth\"), MIN(\"ResolutionWidth\"), MAX(\"ResolutionWidth\"), COUNT(\"ResolutionWidth\"), "
            + "SUM(\"ResolutionHeight\"), MIN(\"ResolutionHeight\"), MAX(\"ResolutionHeight\"), COUNT(\"ResolutionHeight\"), "
            + "SUM(\"ResolutionDepth\"), MIN(\"ResolutionDepth\"), MAX(\"ResolutionDepth\"), COUNT(\"ResolutionDepth\"), "
            + "SUM(\"ClientIP\"), MIN(\"ClientIP\"), MAX(\"ClientIP\"), COUNT(\"ClientIP\"), "
            + "SUM(\"RemoteIP\"), MIN(\"RemoteIP\"), MAX(\"RemoteIP\"), COUNT(\"RemoteIP\"), "
            + "SUM(\"ConnectTiming\"), MIN(\"ConnectTiming\"), MAX(\"ConnectTiming\"), COUNT(\"ConnectTiming\"), "
            + "SUM(\"DNSTiming\"), MIN(\"DNSTiming\"), MAX(\"DNSTiming\"), COUNT(\"DNSTiming\"), "
            + "SUM(\"FetchTiming\"), MIN(\"FetchTiming\"), MAX(\"FetchTiming\"), COUNT(\"FetchTiming\"), "
            + "SUM(\"SendTiming\"), MIN(\"SendTiming\"), MAX(\"SendTiming\"), COUNT(\"SendTiming\") "
            + "FROM mv_input "
            + "GROUP BY \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\"",
        // Ship fields: 5 group keys + (sum + min + max + cnt) × 10 fields = 45
        List.of(
            "EventTime", "RegionID", "OS", "CounterID", "IsRefresh",
            "adv_sum", "adv_min", "adv_max", "adv_cnt",
            "resw_sum", "resw_min", "resw_max", "resw_cnt",
            "resh_sum", "resh_min", "resh_max", "resh_cnt",
            "resd_sum", "resd_min", "resd_max", "resd_cnt",
            "cip_sum", "cip_min", "cip_max", "cip_cnt",
            "rip_sum", "rip_min", "rip_max", "rip_cnt",
            "conn_sum", "conn_min", "conn_max", "conn_cnt",
            "dns_sum", "dns_min", "dns_max", "dns_cnt",
            "fetch_sum", "fetch_min", "fetch_max", "fetch_cnt",
            "send_sum", "send_min", "send_max", "send_cnt"
        )
    );

    /** Fold of {@link #CLICKBENCH_100M}'s state schema on the MV target. */
    public static final MVDefinitionSpec CLICKBENCH_100M_FOLD = new MVDefinitionSpec(
        // All 45 columns are state columns on the target (keys + aggregates)
        List.of(
            new Column("EventTime", ColumnType.INT64),
            new Column("RegionID", ColumnType.INT64),
            new Column("OS", ColumnType.INT64),
            new Column("CounterID", ColumnType.INT64),
            new Column("IsRefresh", ColumnType.INT64),
            new Column("adv_sum", ColumnType.INT64),
            new Column("adv_min", ColumnType.INT64),
            new Column("adv_max", ColumnType.INT64),
            new Column("adv_cnt", ColumnType.INT64),
            new Column("resw_sum", ColumnType.INT64),
            new Column("resw_min", ColumnType.INT64),
            new Column("resw_max", ColumnType.INT64),
            new Column("resw_cnt", ColumnType.INT64),
            new Column("resh_sum", ColumnType.INT64),
            new Column("resh_min", ColumnType.INT64),
            new Column("resh_max", ColumnType.INT64),
            new Column("resh_cnt", ColumnType.INT64),
            new Column("resd_sum", ColumnType.INT64),
            new Column("resd_min", ColumnType.INT64),
            new Column("resd_max", ColumnType.INT64),
            new Column("resd_cnt", ColumnType.INT64),
            new Column("cip_sum", ColumnType.INT64),
            new Column("cip_min", ColumnType.INT64),
            new Column("cip_max", ColumnType.INT64),
            new Column("cip_cnt", ColumnType.INT64),
            new Column("rip_sum", ColumnType.INT64),
            new Column("rip_min", ColumnType.INT64),
            new Column("rip_max", ColumnType.INT64),
            new Column("rip_cnt", ColumnType.INT64),
            new Column("conn_sum", ColumnType.INT64),
            new Column("conn_min", ColumnType.INT64),
            new Column("conn_max", ColumnType.INT64),
            new Column("conn_cnt", ColumnType.INT64),
            new Column("dns_sum", ColumnType.INT64),
            new Column("dns_min", ColumnType.INT64),
            new Column("dns_max", ColumnType.INT64),
            new Column("dns_cnt", ColumnType.INT64),
            new Column("fetch_sum", ColumnType.INT64),
            new Column("fetch_min", ColumnType.INT64),
            new Column("fetch_max", ColumnType.INT64),
            new Column("fetch_cnt", ColumnType.INT64),
            new Column("send_sum", ColumnType.INT64),
            new Column("send_min", ColumnType.INT64),
            new Column("send_max", ColumnType.INT64),
            new Column("send_cnt", ColumnType.INT64)
        ),
        5,  // 5 group keys
        // Fold SQL: SUM sums and counts, MIN mins, MAX maxes
        "SELECT \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\", "
            + "SUM(adv_sum), MIN(adv_min), MAX(adv_max), SUM(adv_cnt), "
            + "SUM(resw_sum), MIN(resw_min), MAX(resw_max), SUM(resw_cnt), "
            + "SUM(resh_sum), MIN(resh_min), MAX(resh_max), SUM(resh_cnt), "
            + "SUM(resd_sum), MIN(resd_min), MAX(resd_max), SUM(resd_cnt), "
            + "SUM(cip_sum), MIN(cip_min), MAX(cip_max), SUM(cip_cnt), "
            + "SUM(rip_sum), MIN(rip_min), MAX(rip_max), SUM(rip_cnt), "
            + "SUM(conn_sum), MIN(conn_min), MAX(conn_max), SUM(conn_cnt), "
            + "SUM(dns_sum), MIN(dns_min), MAX(dns_max), SUM(dns_cnt), "
            + "SUM(fetch_sum), MIN(fetch_min), MAX(fetch_max), SUM(fetch_cnt), "
            + "SUM(send_sum), MIN(send_min), MAX(send_max), SUM(send_cnt) "
            + "FROM mv_input "
            + "GROUP BY \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\"",
        // Same ship fields
        List.of(
            "EventTime", "RegionID", "OS", "CounterID", "IsRefresh",
            "adv_sum", "adv_min", "adv_max", "adv_cnt",
            "resw_sum", "resw_min", "resw_max", "resw_cnt",
            "resh_sum", "resh_min", "resh_max", "resh_cnt",
            "resd_sum", "resd_min", "resd_max", "resd_cnt",
            "cip_sum", "cip_min", "cip_max", "cip_cnt",
            "rip_sum", "rip_min", "rip_max", "rip_cnt",
            "conn_sum", "conn_min", "conn_max", "conn_cnt",
            "dns_sum", "dns_min", "dns_max", "dns_cnt",
            "fetch_sum", "fetch_min", "fetch_max", "fetch_cnt",
            "send_sum", "send_min", "send_max", "send_cnt"
        )
    );
}
