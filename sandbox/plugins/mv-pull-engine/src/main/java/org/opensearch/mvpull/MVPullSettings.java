/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

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
 * GROUP BY group}. The engine is selected for any index that declares
 * {@link #SOURCE_INDEX}.
 */
public final class MVPullSettings {

    private MVPullSettings() {}

    /** Name of the source index whose remote segment store this MV polls. Presence selects the engine. */
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

    /** Numeric group-by field present in the source mapping. */
    public static final Setting<String> GROUP_FIELD = Setting.simpleString(
        "index.mv_pull.group_field",
        "RegionID",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /** Numeric field summed by the definition. */
    public static final Setting<String> SUM_FIELD = Setting.simpleString(
        "index.mv_pull.sum_field",
        "AdvEngineID",
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
    }
}
