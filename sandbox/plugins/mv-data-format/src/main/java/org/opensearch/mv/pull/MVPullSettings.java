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
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Supplier;

/**
 * Node-level services and index settings for the pull-model MV engine POC.
 *
 * <p>Track A scope: single-shard source and MV, numeric group/sum fields,
 * append-only source, definition = {@code SELECT group, COUNT(*), SUM(sum)
 * GROUP BY group}. The engine is selected for any index that declares a
 * {@link DerivedIndexBinding} (via the public
 * {@link DerivedIndexBinding#KEY_SOURCE_NAME} setting) and uses
 * {@code mv_state} as a secondary data format (with {@code parquet} as
 * primary).
 */
public final class MVPullSettings {

    private MVPullSettings() {}

    /**
     * <b>Deprecated.</b> Retained for BWC setting registration only.
     * <p>Do NOT use this setting to gate engine selection or identify the
     * source index at runtime. All pull targets use
     * {@link DerivedIndexBinding} exclusively — the source identity is
     * resolved from the public {@code index.derived.source.name} setting
     * and enriched with private UUID/topology by
     * {@link org.opensearch.cluster.metadata.MetadataCreateIndexService}
     * at creation time.
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

    /** Node services captured by the plugin, handed to engine instances. */
    public record Services(ClusterService clusterService, ThreadPool threadPool, Supplier<RepositoriesService> repositoriesService) {
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
         *
         * <p><b>Thread safety:</b> this method calls
         * {@link ClusterService#state()} and must NOT be invoked from the
         * cluster-applier thread. It is safe to call from GENERIC, poller
         * threads, or any non-applier context.
         */
        public IndexMetadata resolveAndValidateSource(DerivedIndexBinding binding) {
            IndexMetadata md = clusterService.state().metadata().index(binding.sourceName());
            DerivedIndexBinding.ValidationResult result = binding.validateLive(md);
            if (result.isValid() == false) {
                throw new IllegalStateException("mv_pull: " + result.reason());
            }
            return md;
        }
    }
}
