/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.Supplier;

/**
 * Node-level services and index settings for the pull-model MV engine.
 *
 * <p><b>Stage 2:</b> {@link Services} now carries the shared DataFusion
 * runtime pointer and the OpenSearch parent circuit breaker, so
 * {@link MVBuildRuntime} can route builds through the managed runtime and
 * account memory against the breaker.
 */
public final class MVPullSettings {

    private MVPullSettings() {}

    /**
     * <b>Deprecated.</b> Retained for BWC setting registration only.
     *
     * @deprecated Use {@link DerivedIndexBinding#KEY_SOURCE_NAME} instead.
     */
    @Deprecated
    public static final Setting<String> SOURCE_INDEX = Setting.simpleString(
        "index.mv_pull.source_index",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /** Poll cadence. */
    public static final Setting<TimeValue> PULL_INTERVAL = Setting.timeSetting(
        "index.mv_pull.interval",
        TimeValue.timeValueMillis(200),
        TimeValue.timeValueMillis(10),
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Persisted hash of the {@link org.opensearch.mv.MVCompiledDefinition}
     * used to create this MV index. Validated at startup, poll, search, and
     * merge time to detect schema drift.
     */
    public static final Setting<String> DEFINITION_HASH = Setting.simpleString(
        "index.mv_pull.definition_hash",
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    // ── Compaction ─────────────────────────────────────────────────────────

    /**
     * Maximum number of mv_state generations (catalog segments) before
     * background compaction is triggered. When the segment count exceeds
     * this threshold after a successful publish, the compaction service
     * k-way merges the oldest generations into one via the Stage-4
     * streaming merge engine.
     *
     * <p>Default {@code 8}. Set higher to delay compaction (more
     * generations in the catalog, larger restart CRC cost, larger
     * fsync/blob-list surface). Set lower to compact sooner (more CPU
     * spent on compaction, but smaller catalog).
     */
    public static final Setting<Integer> MAX_GENERATIONS_BEFORE_COMPACT = Setting.intSetting(
        "index.mv_pull.max_generations_before_compact",
        8,
        2,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    // ── Bounded streaming rounds ─────────────────────────────────────────

    /**
     * Maximum number of source documents (seq-no range size) processed per
     * pull round. When the lag exceeds this cap, each round builds only a
     * chunk of size {@code max_docs_per_round} and the poller immediately
     * starts the next round (no interval wait) until the lag is drained.
     * This bounds memory to O(chunk) and produces one generation per chunk
     * which the existing compaction machinery folds.
     *
     * <p>Default {@link Long#MAX_VALUE} (uncapped — full range per round).
     * Set to e.g. 2_000_000 for production workloads.
     */
    public static final Setting<Long> MAX_DOCS_PER_ROUND = Setting.longSetting(
        "index.mv_pull.max_docs_per_round",
        Long.MAX_VALUE,
        1L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    // ── Pull-round admission settings (Stage 5, criteria H) ─────────────

    /**
     * Maximum total source bytes (sum of staged parquet file sizes) per pull
     * round. Rounds whose staged input exceeds this limit are rejected with
     * a safe fail-open result (the poller retries next interval). Use
     * {@link Long#MAX_VALUE} (default) to disable.
     */
    public static final Setting<Long> MAX_SOURCE_BYTES_PER_ROUND = Setting.longSetting(
        "index.mv_pull.admission.max_source_bytes",
        Long.MAX_VALUE,
        0L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum estimated operations (row count in the source range) per pull
     * round. Provides a coarse cardinality guard that avoids builds whose
     * result set would OOM the fold. Default {@link Long#MAX_VALUE} (no-op).
     */
    public static final Setting<Long> MAX_OPS_ESTIMATE_PER_ROUND = Setting.longSetting(
        "index.mv_pull.admission.max_ops_estimate",
        Long.MAX_VALUE,
        0L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum cardinality estimate (distinct group keys expected in the
     * output) per pull round. Limits memory used by the HashAggregate
     * during partial fold. Default {@link Long#MAX_VALUE} (no-op).
     */
    public static final Setting<Long> MAX_CARDINALITY_ESTIMATE_PER_ROUND = Setting.longSetting(
        "index.mv_pull.admission.max_cardinality_estimate",
        Long.MAX_VALUE,
        0L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Maximum native-memory pressure (RSS fraction 0.0–1.0) at which new
     * pull rounds are admitted. When jemalloc RSS exceeds this fraction of
     * the node's native-memory budget, the round is rejected to protect
     * running builds. Default 1.0 (no-op / disabled).
     */
    public static final Setting<Double> MAX_NATIVE_PRESSURE_FRACTION = Setting.doubleSetting(
        "index.mv_pull.admission.max_native_pressure",
        1.0,
        0.0,
        1.0,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Returns all admission-related settings for use in plugin registration.
     */
    public static List<Setting<?>> admissionSettings() {
        return List.of(
            MAX_DOCS_PER_ROUND,
            MAX_SOURCE_BYTES_PER_ROUND,
            MAX_OPS_ESTIMATE_PER_ROUND,
            MAX_CARDINALITY_ESTIMATE_PER_ROUND,
            MAX_NATIVE_PRESSURE_FRACTION,
            MAX_GENERATIONS_BEFORE_COMPACT
        );
    }

    /**
     * Node services captured by the plugin, handed to engine instances.
     *
     * <p><b>Stage 2 additions:</b>
     * <ul>
     *   <li>{@link #dataFusionRuntimePtr()} — the shared DataFusionRuntime
     *       native pointer for managed MV builds (replaces the POC's per-call
     *       unbounded SessionContext).</li>
     *   <li>{@link #parentCircuitBreaker()} — the OpenSearch parent circuit
     *       breaker for MV build memory accounting. May be null if the breaker
     *       service is not available (e.g. in unit tests).</li>
     * </ul>
     */
    public record Services(ClusterService clusterService, ThreadPool threadPool, Supplier<RepositoriesService> repositoriesService,
        long dataFusionRuntimePtr, CircuitBreaker parentCircuitBreaker,
        org.opensearch.transport.client.Client client) {
        /**
         * Backward-compatible constructor without Stage 2 services or client.
         * Used by tests and legacy code paths that don't need managed builds.
         */
        public Services(ClusterService clusterService, ThreadPool threadPool, Supplier<RepositoriesService> repositoriesService) {
            this(clusterService, threadPool, repositoriesService, 0L, null, null);
        }

        /**
         * Constructor without client — for existing call sites that don't need cold-start RPC.
         */
        public Services(ClusterService clusterService, ThreadPool threadPool, Supplier<RepositoriesService> repositoriesService,
            long dataFusionRuntimePtr, CircuitBreaker parentCircuitBreaker) {
            this(clusterService, threadPool, repositoriesService, dataFusionRuntimePtr, parentCircuitBreaker, null);
        }

        /** The node's fixed segments path prefix — MUST match what the source's own uploads use. */
        public String segmentsPathFixedPrefix() {
            return org.opensearch.indices.RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(clusterService.getSettings());
        }

        public IndexMetadata sourceIndexMetadata(String sourceIndexName) {
            IndexMetadata md = clusterService.state().metadata().index(sourceIndexName);
            if (md == null) {
                throw new IllegalStateException("mv_pull: source index [" + sourceIndexName + "] does not exist");
            }
            return md;
        }

        /**
         * Resolve and validate the source index against a durable binding.
         * Fails closed on UUID mismatch or topology change.
         */
        public IndexMetadata resolveAndValidateSource(DerivedIndexBinding binding) {
            IndexMetadata md = clusterService.state().metadata().index(binding.sourceName());
            DerivedIndexBinding.ValidationResult result = binding.validateLive(md);
            if (result.isValid() == false) {
                throw new IllegalStateException("mv_pull: " + result.reason());
            }
            return md;
        }

        /**
         * Returns the shared DataFusionRuntime pointer for managed MV builds.
         * Returns 0 if the DataFusion service is not available (graceful degradation
         * to POC path).
         */
        public long dataFusionRuntimePtr() {
            return dataFusionRuntimePtr;
        }

        /**
         * Returns the OpenSearch parent circuit breaker for MV build memory
         * accounting. May be null if the circuit breaker service is not available.
         */
        public CircuitBreaker parentCircuitBreaker() {
            return parentCircuitBreaker;
        }

        /**
         * Returns the transport client for cold-start RPC (checkpoint request
         * action). May be null in unit tests — callers must null-check.
         */
        public org.opensearch.transport.client.Client client() {
            return client;
        }
    }
}
