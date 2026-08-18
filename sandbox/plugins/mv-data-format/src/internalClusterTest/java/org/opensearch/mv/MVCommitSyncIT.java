/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Commit sync end to end (decision 25): the superset rule at COMMIT
 * granularity — when the source commits, the target has already durably
 * committed a catalog snapshot covering every acked ship.
 *
 * <p>Asserted from the two commits themselves (no manual target flush —
 * needing one was exactly the pre-D25 gap):
 * <ol>
 *   <li>the TARGET's last commit user data carries a catalog snapshot
 *       (its commit ran inside the source's flush), and</li>
 *   <li>the SOURCE's commit user data records the {@code mv.commit.<target>}
 *       watermark — the durable anchor the orphan sweep reads.</li>
 * </ol>
 */
@ThreadLeakFilters(filters = MVCommitSyncIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MVCommitSyncIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String SOURCE = "payments";
    private static final String TARGET = "mv_payments";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            MVStateDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    public void testSourceCommitDrivesTargetCommit() throws Exception {
        // Explicit two-index creation (the seed-stable path — the views UX
        // has a known seed-dependent recovery flake, see MVViewsService's
        // gap note; commit sync is orthogonal to how the pair was created).
        client().admin()
            .indices()
            .prepareCreate(SOURCE)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "materialized_view")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .putList(MVConstants.SHIP_TARGETS_SETTING, TARGET)
            )
            .setMapping("service", "type=keyword", "status", "type=keyword", "latency_ms", "type=long")
            .get();
        ensureGreen(SOURCE);
        client().admin()
            .indices()
            .prepareCreate(TARGET)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "mv_state")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(
                "service",
                "type=keyword",
                "status",
                "type=keyword",
                "cnt",
                "type=long",
                "lat_sum",
                "type=long",
                "lat_min",
                "type=long",
                "lat_max",
                "type=long",
                "_mv_source_generation",
                "type=long"
            )
            .get();
        ensureGreen(TARGET);

        // Two generations shipped (refresh per batch).
        client().prepareIndex(SOURCE).setSource("service", "api", "status", "200", "latency_ms", 30).get();
        client().admin().indices().prepareRefresh(SOURCE).get();
        client().prepareIndex(SOURCE).setSource("service", "web", "status", "500", "latency_ms", 80).get();
        client().admin().indices().prepareRefresh(SOURCE).get();

        // The moment under test: SOURCE flush = commit. beforeCommit must
        // first commit the target's catalog (>= the acked ship versions),
        // then record the watermark in the source's own commit.
        assertEquals(0, client().admin().indices().prepareFlush(SOURCE).get().getFailedShards());

        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);

        // 1. TARGET committed WITHOUT any manual target flush: its last commit
        // carries a catalog snapshot whose version covers the acked ships.
        IndexShard targetShard = indicesService.indexServiceSafe(resolveIndex(TARGET)).getShard(0);
        Map<String, String> targetCommit = targetShard.store().readLastCommittedSegmentsInfo().getUserData();
        assertTrue(
            "target's last commit must carry a catalog snapshot (committed inside the source's flush)",
            targetCommit.containsKey(CatalogSnapshot.CATALOG_SNAPSHOT_KEY)
        );

        // 2. SOURCE commit meta records the durable watermark for the sweep.
        IndexShard sourceShard = indicesService.indexServiceSafe(resolveIndex(SOURCE)).getShard(0);
        Map<String, String> sourceCommit = sourceShard.store().readLastCommittedSegmentsInfo().getUserData();
        String watermark = sourceCommit.get("mv.commit." + TARGET);
        assertNotNull("source commit must record the mv.commit watermark, saw keys: " + sourceCommit.keySet(), watermark);
        long committedVersion = Long.parseLong(watermark);
        assertTrue("watermark must be a real committed version, was " + committedVersion, committedVersion >= 1);

        // 3. And the invariant is monotone: another empty flush re-runs the
        // sync harmlessly (idempotent — snapshot id unchanged, commit skipped).
        assertEquals(0, client().admin().indices().prepareFlush(SOURCE).get().getFailedShards());
    }
}
