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
            case "clickbench_q9" -> CLICKBENCH_Q9;
            default -> throw new IllegalArgumentException("unknown mv definition [" + name + "]");
        };
    }

    public static MVDefinitionSpec fold(String name) {
        return switch (name) {
            case "payments" -> TARGET_FOLD;
            case "clickbench_q9" -> CLICKBENCH_Q9_FOLD;
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
}
