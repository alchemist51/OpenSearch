/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

/**
 * POC hardcoded constants — the single fixed materialized view.
 *
 * <p>MV definition: {@code SELECT service, COUNT(*) FROM <table> GROUP BY service}
 * over an index with a single keyword field {@code service}.
 *
 * <p>State file schema (DataFusion Partial output, verified by
 * mv_state_roundtrip_test.rs): {@code service: utf8 | count(Int64(1))[count]: int64}.
 */
public final class MVConstants {

    private MVConstants() {}

    /** The hardcoded MV query. Table name must match what the session registers. */
    public static final String MV_SQL = "SELECT service, COUNT(*) FROM %s GROUP BY service ORDER BY service";

    /** Group-by column of the fixed MV. */
    public static final String GROUP_KEY = "service";

    /** DataFusion state-column name for COUNT(*) (count(*) lowers to count(1)). */
    public static final String COUNT_STATE_COL = "count(Int64(1))[count]";

    /** Directory name under the shard data path; also the format name. */
    public static final String DIR = MVDataFormat.NAME;

    /** MV state file name for a writer generation. */
    public static String mvFileName(long writerGeneration) {
        return "_mv_poc_" + Long.toHexString(writerGeneration) + ".mv.parquet";
    }
}
