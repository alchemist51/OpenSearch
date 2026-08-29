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

    /** Canonical table name every definition SQL is written against. */
    public static final String INPUT_TABLE = "mv_input";

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
     * User-facing MV declaration (decisions 20/22/23): list of
     * {@code definition} or {@code definition:targetName} entries on the
     * SOURCE index. Everything else (formats, ship targets, the target index
     * itself) is derived — see {@link MVViewsService}.
     */
    public static final String VIEWS_SETTING = "index.mv.views";

    /**
     * Validation-scoped read gate (dynamic, on the MV TARGET index): when
     * true, analytics scans of this index serve the catalog snapshot's
     * mv_state Arrow files as the fragment's PARTIAL output (strict — any
     * misalignment throws). The query keeps the ORIGINAL definition shape;
     * the coordinator Final computes final answers (incl. avg) natively.
     */
    public static final String SERVE_STATE_SETTING = "index.mv.serve_state";

    /**
    /**
     * Ordered logical names for columns in each MV state row. This is the durable
     * bridge between the positional Arrow aggregate-state contract and the target
     * index mapping; readers must not infer this order from mapping serialization or
     * DataFusion-generated physical field names.
     */
    public static final String STATE_FIELDS_SETTING = "index.mv.state_fields";

    /** Index setting naming the MV definition (POC named-spec registry). */
    public static final String DEFINITION_SETTING = "index.mv.definition";

    /** Marks a target as a first-class derived index with replication-only writes and no active translog. */
    public static final String DERIVED_INDEX_SETTING = org.opensearch.index.engine.DerivedIndexEngine.DERIVED_INDEX_SETTING;
    /**
     * Commit user-data key prefix for each target/source-shard exact durable
     * claim: {@code mv.cursor.<sourceIndex>.<sourceShard>} maps to a
     * compatibility generation/floor cursor plus encoded above-floor ranges.
     */
    public static final String CURSOR_KEY_PREFIX = "mv.cursor.";

    /** Source commit user-data key containing exact sequence coverage of known no-op operations. */
    public static final String SOURCE_NOOP_COVERAGE_KEY = "mv.source.noop_coverage";

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

    /** MV state file name for a writer generation (Arrow IPC — decision 17). */
    public static String mvFileName(long writerGeneration) {
        return "_mv_poc_" + Long.toHexString(writerGeneration) + ".mv.arrow";
    }
}
