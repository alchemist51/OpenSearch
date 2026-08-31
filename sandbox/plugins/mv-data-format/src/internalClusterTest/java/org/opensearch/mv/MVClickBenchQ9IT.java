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
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

/**
 * ClickBench q9 as the separate-index MV's optimization target, with the REAL
 * {@code hits} mapping types (KB clickbench-reference: {@code RegionID}
 * integer, {@code AdvEngineID}/{@code ResolutionWidth} short).
 *
 * <p>Reference query:
 * {@code SELECT RegionID, SUM(AdvEngineID), COUNT(*) AS c, AVG(ResolutionWidth)
 * FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10}.
 * The MV definition stores COUNT/SUM(Adv)/SUM(Res)/MIN/MAX states; AVG is
 * DECOMPOSED — the read computes {@code SUM(res_sum)/SUM(cnt)} exactly.
 *
 * <p>Validated against the real 100M-row ClickBench parquet on the benchmark
 * node (datafusion-cli): direct q9 ≡ partial-state → fold → final, row for
 * row (incl. AVG), 99,997,497 raw rows → 9,040 state rows (11,062×).
 * This IT proves the same algebra through the LIVE cluster path:
 * ingest → ship-before-commit → target fold → final over mv_state files.
 */
@ThreadLeakFilters(filters = MVClickBenchQ9IT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class MVClickBenchQ9IT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String SOURCE = "hits";
    private static final String TARGET = "mv_hits_q9";

    /** Final fold over the target's mv_state files (CLICKBENCH_Q9_FOLD state names). */
    private static final String Q9_FOLD_SEARCH_SQL = "SELECT \"RegionID\", "
        + "SUM(\"sum(mv_input.cnt)[sum]\") AS cnt, "
        + "SUM(\"sum(mv_input.adv_sum)[sum]\") AS adv, "
        + "SUM(\"sum(mv_input.res_sum)[sum]\") AS res_sum, "
        + "MIN(\"min(mv_input.res_min)[value]\") AS res_min, "
        + "MAX(\"max(mv_input.res_max)[value]\") AS res_max "
        + "FROM __MV_STATES__ GROUP BY \"RegionID\" ORDER BY cnt DESC, \"RegionID\"";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .build();
    }

    public void testQ9ServedFromShippedFoldedState() throws Exception {
        // Source: the ClickBench composite shape (parquet primary) with the
        // REAL mapping types for the definition's fields, q9 definition wired.
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
                    .put(MVConstants.DEFINITION_SETTING, "clickbench_q9")
                    .putList(MVConstants.SHIP_TARGETS_SETTING, TARGET)
            )
            // Real hits mapping types (clickbench-reference): integer + short + short.
            .setMapping("RegionID", "type=integer", "AdvEngineID", "type=short", "ResolutionWidth", "type=short")
            .get();
        ensureGreen(SOURCE);

        client().admin()
            .indices()
            .prepareCreate(TARGET)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MVConstants.DERIVED_INDEX_SETTING, true)
                    .put("index.append_only.enabled", true)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DATA_FORMAT, "materialized_view")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.DEFINITION_SETTING, "clickbench_q9")
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(MVViewsService.TargetCreator.targetMapping("clickbench_q9"))
            .get();
        ensureGreen(TARGET);

        // Generation 1: region 229 x2, region 2 x1.
        indexHit(229, 1, 100);
        indexHit(229, 0, 200);
        indexHit(2, 5, 1000);
        client().admin().indices().prepareRefresh(SOURCE).get();

        // Generation 2: region 229 x1 (group spans generations), region 208 x1.
        indexHit(229, 2, 300);
        indexHit(208, 3, 500);
        client().admin().indices().prepareRefresh(SOURCE).get();

        // q9 through the MV: final fold over the target's mv_state files.
        // Goldens (hand-computed):
        // 229: cnt=3 adv=3 res_sum=600 (AVG=200) min=100 max=300
        // 2: cnt=1 adv=5 res_sum=1000 (AVG=1000)
        // 208: cnt=1 adv=3 res_sum=500 (AVG=500)
        java.util.List<String> stateFiles = targetStateFiles();
        assertFalse("target must carry mv_state files", stateFiles.isEmpty());
        String result = MVNativeBridge.searchV2(stateFiles, Q9_FOLD_SEARCH_SQL);
        assertEquals("229\t3\t3\t600\t100\t300\n" + "2\t1\t5\t1000\t1000\t1000\n" + "208\t1\t3\t500\t500\t500\n", result);

        // AVG decomposition, spelled out: AVG(ResolutionWidth) for 229 is
        // res_sum/cnt = 600/3 = 200 — exact, from mergeable states only.
        String[] region229 = result.split("\n")[0].split("\t");
        assertEquals(200L, Long.parseLong(region229[3]) / Long.parseLong(region229[1]));
    }

    private java.util.List<String> targetStateFiles() throws Exception {
        String nodeName = getClusterState().nodes()
            .get(getClusterState().routingTable().index(TARGET).shard(0).primaryShard().currentNodeId())
            .getName();
        org.opensearch.indices.IndicesService indicesService = internalCluster().getInstance(
            org.opensearch.indices.IndicesService.class,
            nodeName
        );
        org.opensearch.index.shard.IndexShard shard = indicesService.indexServiceSafe(getClusterState().metadata().index(TARGET).getIndex())
            .getShard(0);
        org.opensearch.index.engine.DataFormatAwareEngine engine =
            (org.opensearch.index.engine.DataFormatAwareEngine) org.opensearch.index.shard.IndexShardTestCase.getIndexer(shard);
        java.util.List<String> files = new java.util.ArrayList<>();
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = engine
                .acquireSnapshot()
        ) {
            for (org.opensearch.index.engine.exec.WriterFileSet set : ref.get().getSearchableFiles(MVStateDataFormat.NAME)) {
                for (String f : set.files()) {
                    files.add(java.nio.file.Path.of(set.directory()).resolve(f).toString());
                }
            }
        }
        return files;
    }

    private void indexHit(int regionId, int advEngineId, int resolutionWidth) {
        client().prepareIndex()
            .setIndex(SOURCE)
            .setSource("RegionID", regionId, "AdvEngineID", advEngineId, "ResolutionWidth", resolutionWidth)
            .get();
    }
}
