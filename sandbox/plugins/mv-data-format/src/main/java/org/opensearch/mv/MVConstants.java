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
 * POC hardcoded constants — the single fixed materialized view (v2:
 * multi-key, multi-agg).
 *
 * <p>Definition: {@code SELECT service, status, COUNT(*), SUM(latency_ms),
 * MIN(latency_ms), MAX(latency_ms) FROM mv_input GROUP BY service, status}.
 * The table name inside the SQL is always {@code mv_input} — the native
 * writer registers the fed batches under that name.
 *
 * <p>State-file schema comes FROM THE PLAN (state-suffixed columns); Java
 * only knows the group keys and the search template.
 */
public final class MVConstants {

    private MVConstants() {}

    /** The MV definition executed by the native writer over fed batches. */
    public static final String MV_SQL = "SELECT service, status, COUNT(*), SUM(latency_ms), MIN(latency_ms), MAX(latency_ms) "
        + "FROM mv_input GROUP BY service, status";

    /** Group-by columns (leading state-file columns; also the sort key). */
    public static final List<String> GROUP_KEYS = List.of("service", "status");

    /** Metric column captured alongside the keys. */
    public static final String METRIC_FIELD = "latency_ms";

    /**
     * Search template over state files: Final-fold of the state columns.
     * {@code __MV_STATES__} is replaced natively with the UNION ALL of the
     * snapshot's state files. State column names are DataFusion's
     * format_state_name output for the MV_SQL aggregates.
     */
    public static final String SEARCH_SQL = "SELECT service, status, "
        + "SUM(\"count(Int64(1))[count]\") AS cnt, "
        + "SUM(\"sum(mv_input.latency_ms)[sum]\") AS lat_sum, "
        + "MIN(\"min(mv_input.latency_ms)[value]\") AS lat_min, "
        + "MAX(\"max(mv_input.latency_ms)[value]\") AS lat_max "
        + "FROM __MV_STATES__ GROUP BY service, status ORDER BY service, status";

    /** Directory name under the shard data path; also the format name. */
    public static final String DIR = MVDataFormat.NAME;

    /**
     * Raw state export over a finalized state file (separate-index ship path):
     * SELECT of the state columns verbatim — no folding, the MV index folds on
     * read. Order matches {@link #SHIP_FIELDS}.
     */
    public static final String EXPORT_SQL = "SELECT service, status, "
        + "\"count(Int64(1))[count]\", "
        + "\"sum(mv_input.latency_ms)[sum]\", "
        + "\"min(mv_input.latency_ms)[value]\", "
        + "\"max(mv_input.latency_ms)[value]\" "
        + "FROM __MV_STATES__ ORDER BY service, status";

    /** MV-index document field names, parallel to {@link #EXPORT_SQL} columns. */
    public static final List<String> SHIP_FIELDS = List.of("service", "status", "cnt", "lat_sum", "lat_min", "lat_max");

    /**
     * Source index setting listing the target MV indices; non-empty enables the
     * ship path. Multiple targets share ONE finalized state batch via
     * {@link MVRefCountedStateBatch} — the flush commits only when EVERY
     * target has acked (the invariant holds per target).
     */
    public static final String SHIP_TARGETS_SETTING = "index.mv.ship_targets";

    /**
     * Target index setting naming the SOURCE index whose primaries this
     * index's primaries must colocate with (ordinal 1:1 pairing). Consumed by
     * {@link MVColocationAllocationDecider}.
     */
    public static final String COLOCATE_WITH_SETTING = "index.mv.colocate_with";

    /**
     * Final fold over the TARGET's mv_state files (state column names from the
     * TARGET_FOLD definition, DataFusion format_state_name output).
     */
    public static final String TARGET_FOLD_SEARCH_SQL = "SELECT service, status, "
        + "SUM(\"sum(mv_input.cnt)[sum]\") AS cnt, "
        + "SUM(\"sum(mv_input.lat_sum)[sum]\") AS lat_sum, "
        + "MIN(\"min(mv_input.lat_min)[value]\") AS lat_min, "
        + "MAX(\"max(mv_input.lat_max)[value]\") AS lat_max "
        + "FROM __MV_STATES__ GROUP BY service, status ORDER BY service, status";

    /** MV state file name for a writer generation. */
    public static String mvFileName(long writerGeneration) {
        return "_mv_poc_" + Long.toHexString(writerGeneration) + ".mv.parquet";
    }
}
