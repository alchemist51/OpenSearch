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

    /** MV state file name for a writer generation. */
    public static String mvFileName(long writerGeneration) {
        return "_mv_poc_" + Long.toHexString(writerGeneration) + ".mv.parquet";
    }
}
