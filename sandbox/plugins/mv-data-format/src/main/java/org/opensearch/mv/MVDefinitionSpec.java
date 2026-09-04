/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

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
            case "clickbench_5m_url" -> CLICKBENCH_5M_URL;
            case "heavy_l1" -> HEAVY_L1;
            case "heavy_l2" -> HEAVY_L2;
            case "heavy_l3" -> HEAVY_L3;
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
            case "clickbench_5m_url" -> CLICKBENCH_5M_URL_FOLD;
            case "heavy_l1" -> HEAVY_L1_FOLD;
            case "heavy_l2" -> HEAVY_L2_FOLD;
            case "heavy_l3" -> HEAVY_L3_FOLD;
            default -> throw new IllegalArgumentException("unknown mv fold definition [" + name + "]");
        };
    }

    /** All registered definition names, for test enumeration. */
    public static List<String> allNames() {
        return List.of(
            "payments",
            "pull_count_sum",
            "pull_count_sum_userid",
            "clickbench_q9",
            "clickbench_q9_native",
            "clickbench_100m",
            "clickbench_5m_url",
            "heavy_l1",
            "heavy_l2",
            "heavy_l3"
        );
    }

    /**
     * Synthesize the FOLD spec of a compiled definition — the target-side spec
     * for DESCRIPTOR-ONLY targets (created via {@code PUT /_mv/views}), which
     * deliberately carry no named-registry id. Input columns are the
     * definition's state columns (stable aliases); the SQL is the generated
     * fold. Legacy {@link ColumnType} has no TIMESTAMP, so non-keyword keys
     * approximate to INT64 — acceptable because pull-path targets never feed
     * these columns through the ship-receive buffers.
     */
    public static MVDefinitionSpec foldOf(MVCompiledDefinition def) {
        java.util.List<Column> cols = new java.util.ArrayList<>();
        for (GroupKey k : def.groupKeys()) {
            cols.add(new Column(k.name(), k.columnType() == GroupKey.ColumnType.KEYWORD ? ColumnType.UTF8 : ColumnType.INT64));
        }
        for (AggregateSpec a : def.aggregates()) {
            for (AggregateSpec.StateColumn sc : a.stateColumns()) {
                cols.add(new Column(sc.name(), ColumnType.INT64));
            }
        }
        return new MVDefinitionSpec(
            java.util.List.copyOf(cols),
            def.groupKeys().size(),
            def.buildFoldSql(MVConstants.INPUT_TABLE),
            def.projectionOrder()
        );
    }

    /** The source index's definition (raw fields → state). SQL + ship fields GENERATED from the typed definition. */
    public static final MVDefinitionSpec SOURCE = new MVDefinitionSpec(
        List.of(new Column("service", ColumnType.UTF8), new Column("status", ColumnType.UTF8), new Column("latency_ms", ColumnType.INT64)),
        2,
        MVCompiledDefinition.payments().buildPartialSql(MVConstants.INPUT_TABLE),
        MVCompiledDefinition.payments().projectionOrder()
    );

    /** The target index's definition: the FOLD of the shipped state schema. SQL GENERATED from the typed fold definition. */
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
        MVCompiledDefinition.paymentsFold().buildPartialSql(MVConstants.INPUT_TABLE),
        MVCompiledDefinition.paymentsFold().projectionOrder()
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

    /**
     * Wide-column 100M ClickBench MV definition: 5 group keys
     * (EventTime, RegionID, OS, CounterID, IsRefresh) and 10 metric
     * fields each with SUM/MIN/MAX/COUNT = 45 output columns total.
     * Used for the 100M catch-up benchmark.
     *
     * <p>SQL, ship fields, and fold are derived from the single authoritative
     * {@link MVCompiledDefinition#clickbench100m()} — there is no second
     * hand-written copy that could drift.</p>
     */
    private static final MVCompiledDefinition CLICKBENCH_100M_COMPILED = MVCompiledDefinition.clickbench100m();

    public static final MVDefinitionSpec CLICKBENCH_100M = sourceFromCompiled(
        CLICKBENCH_100M_COMPILED,
        // Columns: 5 group keys + 10 metric source fields captured from the broadcast
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
        )
    );

    /** Fold of {@link #CLICKBENCH_100M}'s state schema on the target. */
    public static final MVDefinitionSpec CLICKBENCH_100M_FOLD = foldFromCompiled(CLICKBENCH_100M_COMPILED);

    // ── Heavy-MV saturation ladder ──────────────────────────────────────
    // L0 = CLICKBENCH_100M (existing 45-col definition).
    // L1 = wider group keys (8), same 10 metrics × 4 aggs = 48 output cols.
    // L2 = 8 group keys, 20 metrics × 4 aggs = 88 output cols.
    // L3 = 10 group keys (2 UTF8), 30 metrics × 4 aggs = 130 output cols.

    // ── metric descriptors ──────────────────────────────────────────────

    /** A metric field and its short alias prefix used in ship-field naming. */
    private record MetricField(String sourceField, String prefix, ColumnType type) {
    }

    /** L0/L1 metrics: same 10 numeric fields as CLICKBENCH_100M. */
    private static final List<MetricField> METRICS_10 = List.of(
        new MetricField("AdvEngineID", "adv", ColumnType.INT64),
        new MetricField("ResolutionWidth", "resw", ColumnType.INT64),
        new MetricField("ResolutionHeight", "resh", ColumnType.INT64),
        new MetricField("ResolutionDepth", "resd", ColumnType.INT64),
        new MetricField("ClientIP", "cip", ColumnType.INT64),
        new MetricField("RemoteIP", "rip", ColumnType.INT64),
        new MetricField("ConnectTiming", "conn", ColumnType.INT64),
        new MetricField("DNSTiming", "dns", ColumnType.INT64),
        new MetricField("FetchTiming", "fetch", ColumnType.INT64),
        new MetricField("SendTiming", "send", ColumnType.INT64)
    );

    /** L2 adds 10 more numeric columns. */
    private static final List<MetricField> METRICS_20;
    static {
        var m = new ArrayList<>(METRICS_10);
        m.add(new MetricField("ResponseStartTiming", "rsstart", ColumnType.INT64));
        m.add(new MetricField("ResponseEndTiming", "rsend", ColumnType.INT64));
        m.add(new MetricField("Age", "age", ColumnType.INT64));
        m.add(new MetricField("HID", "hid", ColumnType.INT64));
        m.add(new MetricField("CodeVersion", "codv", ColumnType.INT64));
        m.add(new MetricField("IPNetworkID", "ipnet", ColumnType.INT64));
        m.add(new MetricField("SilverlightVersion3", "sl3", ColumnType.INT64));
        m.add(new MetricField("WindowName", "wnam", ColumnType.INT64));
        m.add(new MetricField("URLHash", "urlh", ColumnType.INT64));
        m.add(new MetricField("RefererHash", "refh", ColumnType.INT64));
        METRICS_20 = List.copyOf(m);
    }

    /** L3 adds 10 more for 30 total (120 aggregate output columns). */
    private static final List<MetricField> METRICS_30;
    static {
        var m = new ArrayList<>(METRICS_20);
        m.add(new MetricField("ParamPrice", "pprice", ColumnType.INT64));
        m.add(new MetricField("UserAgent", "uagent", ColumnType.INT64));
        m.add(new MetricField("UserAgentMajor", "uamaj", ColumnType.INT64));
        m.add(new MetricField("WindowClientWidth", "wcw", ColumnType.INT64));
        m.add(new MetricField("WindowClientHeight", "wch", ColumnType.INT64));
        m.add(new MetricField("Sex", "sex", ColumnType.INT64));
        m.add(new MetricField("Robotness", "robot", ColumnType.INT64));
        m.add(new MetricField("Income", "income", ColumnType.INT64));
        m.add(new MetricField("HistoryLength", "histl", ColumnType.INT64));
        m.add(new MetricField("OpenerName", "opener", ColumnType.INT64));
        METRICS_30 = List.copyOf(m);
    }

    // ── group key descriptors ───────────────────────────────────────────

    /** L0/L1-base 5 group keys (all INT64). */
    private static final List<Column> GK_5 = List.of(
        new Column("EventTime", ColumnType.INT64),
        new Column("RegionID", ColumnType.INT64),
        new Column("OS", ColumnType.INT64),
        new Column("CounterID", ColumnType.INT64),
        new Column("IsRefresh", ColumnType.INT64)
    );

    /** L1/L2: 8 group keys = L0 + UserID + WatchID + FUniqID (all long). */
    private static final List<Column> GK_8;
    static {
        var g = new ArrayList<>(GK_5);
        g.add(new Column("UserID", ColumnType.INT64));
        g.add(new Column("WatchID", ColumnType.INT64));
        g.add(new Column("FUniqID", ColumnType.INT64));
        GK_8 = List.copyOf(g);
    }

    /** L3: 10 group keys = L2 + URL + Referer (UTF8 — variable string bytes). */
    private static final List<Column> GK_10;
    static {
        var g = new ArrayList<>(GK_8);
        g.add(new Column("URL", ColumnType.UTF8));
        g.add(new Column("Referer", ColumnType.UTF8));
        GK_10 = List.copyOf(g);
    }

    // ── builder helpers ──────────────────────────────────────────────────

    /**
     * Build a SOURCE definition from group keys and metric descriptors.
     * Columns = keys + metric source fields. SQL = SELECT keys, SUM/MIN/MAX/COUNT(metric)... GROUP BY keys.
     * Ship fields = keys + (prefix_sum, prefix_min, prefix_max, prefix_cnt) × metrics.
     */
    static MVDefinitionSpec buildSource(List<Column> keys, List<MetricField> metrics) {
        List<Column> columns = new ArrayList<>(keys);
        for (MetricField m : metrics) {
            columns.add(new Column(m.sourceField(), m.type()));
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteId(keys.get(i).name()));
        }
        for (MetricField m : metrics) {
            String q = quoteId(m.sourceField());
            sql.append(String.format(Locale.ROOT, ", SUM(%s), MIN(%s), MAX(%s), COUNT(%s)", q, q, q, q));
        }
        sql.append(" FROM mv_input GROUP BY ");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteId(keys.get(i).name()));
        }

        List<String> ship = new ArrayList<>();
        for (Column k : keys) {
            ship.add(k.name());
        }
        for (MetricField m : metrics) {
            ship.add(m.prefix() + "_sum");
            ship.add(m.prefix() + "_min");
            ship.add(m.prefix() + "_max");
            ship.add(m.prefix() + "_cnt");
        }

        return new MVDefinitionSpec(columns, keys.size(), sql.toString(), ship);
    }

    /**
     * Build a FOLD definition from a source spec. Fold columns are all ship
     * fields (keys are keys; aggregates are state columns). Fold SQL re-aggregates:
     * SUM(sums), MIN(mins), MAX(maxes), SUM(counts).
     */
    static MVDefinitionSpec buildFold(MVDefinitionSpec source, List<Column> keys) {
        List<Column> foldCols = new ArrayList<>();
        for (String sf : source.shipFields()) {
            // Determine column type: keys retain original type, agg states are INT64
            ColumnType ct = ColumnType.INT64;
            for (Column k : keys) {
                if (k.name().equals(sf)) {
                    ct = k.type();
                    break;
                }
            }
            foldCols.add(new Column(sf, ct));
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteId(keys.get(i).name()));
        }

        // Aggregate state columns: 4 per metric (sum, min, max, cnt)
        List<String> aggFields = source.shipFields().subList(keys.size(), source.shipFields().size());
        for (int i = 0; i < aggFields.size(); i += 4) {
            String sumF = aggFields.get(i);
            String minF = aggFields.get(i + 1);
            String maxF = aggFields.get(i + 2);
            String cntF = aggFields.get(i + 3);
            sql.append(String.format(Locale.ROOT, ", SUM(%s), MIN(%s), MAX(%s), SUM(%s)", sumF, minF, maxF, cntF));
        }

        sql.append(" FROM mv_input GROUP BY ");
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(quoteId(keys.get(i).name()));
        }

        return new MVDefinitionSpec(foldCols, keys.size(), sql.toString(), source.shipFields());
    }

    private static String quoteId(String id) {
        return "\"" + id + "\"";
    }

    // ── compiled-definition-derived specs ────────────────────────────────

    /**
     * Build the capture column list (group keys + metric source fields) for a
     * compiled-definition-derived spec.
     */
    static List<Column> buildCaptureColumns(List<Column> keys, List<MetricField> metrics) {
        List<Column> cols = new ArrayList<>(keys);
        for (MetricField m : metrics) {
            cols.add(new Column(m.sourceField(), m.type()));
        }
        return cols;
    }

    /**
     * Build a SOURCE spec whose SQL and ship fields are derived from the
     * single authoritative {@link MVCompiledDefinition}. The partial SQL and
     * the ship-field (state_fields) order therefore always agree with the
     * compiled projection order — there is no second, hand-written copy that
     * could drift. {@code captureColumns} lists the raw broadcast columns the
     * forward buffer reads (group-key source fields + metric source fields).
     */
    static MVDefinitionSpec sourceFromCompiled(MVCompiledDefinition def, List<Column> captureColumns) {
        return new MVDefinitionSpec(
            captureColumns,
            def.groupKeys().size(),
            def.buildPartialSql(MVConstants.INPUT_TABLE),
            def.stateColumnNames()
        );
    }

    /**
     * Build the TARGET FOLD spec for a compiled definition. Fold columns are
     * the materialized state columns (group keys keep their type; aggregate
     * state columns are INT64), the SQL is the compiled fold SQL (grouping by
     * the already-materialized key columns), and ship fields match the
     * compiled projection order.
     */
    static MVDefinitionSpec foldFromCompiled(MVCompiledDefinition def) {
        List<Column> cols = new ArrayList<>();
        for (GroupKey k : def.groupKeys()) {
            ColumnType ct = k.columnType() == GroupKey.ColumnType.KEYWORD ? ColumnType.UTF8 : ColumnType.INT64;
            cols.add(new Column(k.name(), ct));
        }
        for (AggregateSpec a : def.aggregates()) {
            for (AggregateSpec.StateColumn sc : a.stateColumns()) {
                cols.add(new Column(sc.name(), ColumnType.INT64));
            }
        }
        return new MVDefinitionSpec(cols, def.groupKeys().size(), def.buildFoldSql(MVConstants.INPUT_TABLE), def.stateColumnNames());
    }

    // ── clickbench_5m_url: 3 keys (5-min EventTime bucket, URL, UserID) +
    // SUM/MIN/MAX/COUNT × 10 fields = 43 projection columns ─────────────

    /** The authoritative compiled definition backing {@link #CLICKBENCH_5M_URL}. */
    private static final MVCompiledDefinition CLICKBENCH_5M_URL_COMPILED = MVCompiledDefinition.clickbench5mUrl();

    /**
     * Source spec for {@code clickbench_5m_url}. Captured columns are the raw
     * {@code EventTime} (bucketed in SQL), {@code URL}, {@code UserID}, and
     * the ten numeric metric fields. SQL and 43 ship fields are derived from
     * {@link #CLICKBENCH_5M_URL_COMPILED}.
     */
    public static final MVDefinitionSpec CLICKBENCH_5M_URL = sourceFromCompiled(
        CLICKBENCH_5M_URL_COMPILED,
        List.of(
            new Column("EventTime", ColumnType.INT64),
            new Column("URL", ColumnType.UTF8),
            new Column("UserID", ColumnType.INT64),
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
        )
    );

    /** Fold of {@link #CLICKBENCH_5M_URL}'s state schema on the target. */
    public static final MVDefinitionSpec CLICKBENCH_5M_URL_FOLD = foldFromCompiled(CLICKBENCH_5M_URL_COMPILED);

    // ── L1: 8 group keys + 10 metrics × 4 = 48 output columns ─────────

    /** The authoritative compiled definition backing {@link #HEAVY_L1}. */
    private static final MVCompiledDefinition HEAVY_L1_COMPILED = MVCompiledDefinition.heavyL1();

    /**
     * HEAVY L1: wider group keys (EventTime, RegionID, OS, CounterID,
     * IsRefresh, UserID, WatchID, FUniqID) with the same 10 metric fields
     * as L0. 8 keys + 40 aggregates = 48 output columns. SQL, ship fields,
     * and fold are derived from {@link MVCompiledDefinition#heavyL1()}.
     */
    public static final MVDefinitionSpec HEAVY_L1 = sourceFromCompiled(HEAVY_L1_COMPILED, buildCaptureColumns(GK_8, METRICS_10));

    /** Fold of {@link #HEAVY_L1}'s state schema on the target. */
    public static final MVDefinitionSpec HEAVY_L1_FOLD = foldFromCompiled(HEAVY_L1_COMPILED);

    // ── L2: 8 group keys + 20 metrics × 4 = 88 output columns ─────────

    /** The authoritative compiled definition backing {@link #HEAVY_L2}. */
    private static final MVCompiledDefinition HEAVY_L2_COMPILED = MVCompiledDefinition.heavyL2();

    /**
     * HEAVY L2: same 8 group keys as L1, but doubles the aggregate surface
     * with 20 metrics × 4 = 80 aggregate state columns. 8 + 80 = 88
     * output columns total. SQL, ship fields, and fold are derived from
     * {@link MVCompiledDefinition#heavyL2()}.
     */
    public static final MVDefinitionSpec HEAVY_L2 = sourceFromCompiled(HEAVY_L2_COMPILED, buildCaptureColumns(GK_8, METRICS_20));

    /** Fold of {@link #HEAVY_L2}'s state schema on the target. */
    public static final MVDefinitionSpec HEAVY_L2_FOLD = foldFromCompiled(HEAVY_L2_COMPILED);

    // ── L3: 10 group keys (2 UTF8) + 30 metrics × 4 = 130 output columns

    /** The authoritative compiled definition backing {@link #HEAVY_L3}. */
    private static final MVCompiledDefinition HEAVY_L3_COMPILED = MVCompiledDefinition.heavyL3();

    /**
     * HEAVY L3: maximum width. 10 group keys — adds URL and Referer
     * (UTF8/keyword) to the 8 INT64 keys from L2. 30 metrics × 4 = 120
     * aggregate state columns. 10 + 120 = 130 output columns total. SQL,
     * ship fields, and fold are derived from
     * {@link MVCompiledDefinition#heavyL3()}.
     */
    public static final MVDefinitionSpec HEAVY_L3 = sourceFromCompiled(HEAVY_L3_COMPILED, buildCaptureColumns(GK_10, METRICS_30));

    /** Fold of {@link #HEAVY_L3}'s state schema on the target. */
    public static final MVDefinitionSpec HEAVY_L3_FOLD = foldFromCompiled(HEAVY_L3_COMPILED);
}
