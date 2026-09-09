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

    /**
     * The DERIVED DATA-FORMAT CATEGORY value an MV target declares in
     * {@code index.derived.data_format}. This is pure control-plane ROUTING:
     * it keys pull-service eligibility ({@code DerivedPullFormat#formatId()})
     * and the analytics MV-serving dispatch. It is NOT a physical data format —
     * MV state artifacts are stock {@link #STATE_ARTIFACT_FORMAT} files owned
     * by the target's composite primary.
     */
    public static final String DERIVED_CATEGORY = "materialized_view";

    /**
     * The physical format that owns MV state artifacts: the target's composite
     * PRIMARY format. State generations are plain parquet files living in the
     * shard's parquet directory, cataloged/checksummed/uploaded by the stock
     * machinery — nothing MV-specific below the publish call.
     */
    public static final String STATE_ARTIFACT_FORMAT = "parquet";

    /**
     * User-facing MV declaration (decisions 20/22/23): list of
     * {@code definition} or {@code definition:targetName} entries on the
     * SOURCE index. Everything else (formats, the target index itself) is
     * derived — see {@link MVViewsService}.
     */
    public static final String VIEWS_SETTING = "index.mv.views";

    /**
     * Ordered logical names for columns in each MV state row. This is the durable
     * bridge between the positional Arrow aggregate-state contract and the target
     * index mapping; readers must not infer this order from mapping serialization or
     * DataFusion-generated physical field names.
     */
    public static final String STATE_FIELDS_SETTING = "index.mv.state_fields";

    /**
     * Target index setting carrying the persisted, self-contained MV
     * definition descriptor as compact JSON (Stage&nbsp;4). When present it is
     * the authoritative definition source resolved FIRST by
     * {@link MVDefinitionResolver}; {@code index.derived.definition_id}
     * remains a legacy named fallback.
     *
     * <p>The JSON is produced by
     * {@link MVDefinitionDescriptor#toXContent} (which embeds the integrity
     * {@code definition_hash}) and rebuilt via
     * {@link MVCompiledDefinition#fromDescriptor}. A target created with this
     * setting is self-contained across restarts and does not depend on the
     * hardcoded {@link MVCompiledDefinition#compiledFor(String)} switch.
     *
     * <p>Registered as a PUBLIC, {@code Final}, {@code IndexScope} setting so
     * the MV control plane (MVViewsService today; the Stage&nbsp;5 REST create
     * endpoint next) can submit it directly in the create request — exactly
     * like {@code index.derived.definition_id}. Private settings submitted via
     * a client create request are rejected, so this cannot be private.
     */
    public static final String DESCRIPTOR_SETTING = "index.mv.descriptor";

    /** Marks a target as a first-class derived index with replication-only writes and no active translog. */
    public static final String DERIVED_INDEX_SETTING = org.opensearch.index.engine.DerivedIndexEngine.DERIVED_INDEX_SETTING;

    /**
     * Target index setting naming the SOURCE index whose primaries this
     * index's primaries must colocate with (ordinal 1:1 pairing). Consumed by
     * {@link MVColocationAllocationDecider}.
     */
    public static final String COLOCATE_WITH_SETTING = "index.mv.colocate_with";

    /**
     * MV state artifact name for a reserved writer generation (Parquet at
     * rest — Arrow is in-memory only).
     *
     * <p>Mirrors the stock parquet writer's naming convention
     * ({@code _parquet_file_generation_<hex-gen>.parquet}, see
     * {@code ParquetIndexingEngine.buildParquetFileName}) with a {@code _mv}
     * marker in the prefix slot so state generations are grep-distinguishable
     * from doc-sourced files while remaining ordinary parquet catalog entries.
     * The convention is duplicated as a string here deliberately: plugins do
     * not reach into sibling plugin classloaders, and the catalog resolves
     * files by (directory, name) — the name shape is debug-facing only.
     */
    public static String stateFileName(long writerGeneration) {
        return "_parquet_file_generation_mv_" + Long.toHexString(writerGeneration) + ".parquet";
    }
}
