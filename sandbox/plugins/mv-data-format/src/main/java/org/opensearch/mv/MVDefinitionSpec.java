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
 * @param columns   captured columns, group keys first (buffer/state order)
 * @param groupKeys number of leading group-key columns
 * @param sql       definition SQL over table {@code mv_input}, maintained
 *                  Partial-stopped by the native writer
 */
public record MVDefinitionSpec(List<Column> columns, int groupKeys, String sql) {

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
    }

    /** The source index's definition (raw fields → state). */
    public static final MVDefinitionSpec SOURCE = new MVDefinitionSpec(
        List.of(new Column("service", ColumnType.UTF8), new Column("status", ColumnType.UTF8), new Column("latency_ms", ColumnType.INT64)),
        2,
        MVConstants.MV_SQL
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
        "SELECT service, status, SUM(cnt), SUM(lat_sum), MIN(lat_min), MAX(lat_max) FROM mv_input GROUP BY service, status"
    );
}
