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

import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.indices.IndicesService;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * POC(mv) end-to-end ingestion test: composite index with parquet primary and
 * materialized_view secondary; golden dataset; assert the MV state file lands
 * in the segment's snapshot with correct per-segment counts.
 */
@ThreadLeakFilters(filters = MVDataFormatPocIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MVDataFormatPocIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

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
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 1)
            .build();
    }

    public void testMvStateFileBuiltAtFlush() throws Exception {
        String index = "payments";
        client().admin()
            .indices()
            .prepareCreate(index)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "materialized_view")
                    // POC: merges disabled — keep flush segments pure
                    .put("index.composite.merge_on_refresh_max_size", "0b")
            )
            .setMapping("service", "type=keyword")
            .get();
        ensureGreen(index);

        // Golden segment 1: api,api,web,api,api -> api:4, web:1
        for (String svc : List.of("api", "api", "web", "api", "api")) {
            IndexResponse r = client().prepareIndex().setIndex(index).setSource("service", svc).get();
            assertEquals(RestStatus.CREATED, r.status());
        }
        client().admin().indices().prepareRefresh(index).get();

        // Golden segment 2: api,web,batch -> api:1, web:1, batch:1
        for (String svc : List.of("api", "web", "batch")) {
            IndexResponse r = client().prepareIndex().setIndex(index).setSource("service", svc).get();
            assertEquals(RestStatus.CREATED, r.status());
        }
        client().admin().indices().prepareRefresh(index).get();

        // ---- Assert: every segment carries a materialized_view WriterFileSet ----
        IndexShard shard = getPrimaryShard(index);
        DataFormatAwareEngine engine = (DataFormatAwareEngine) IndexShardTestCase.getIndexer(shard);
        try (GatedCloseable<CatalogSnapshot> ref = engine.acquireSnapshot()) {
            CatalogSnapshot snapshot = ref.get();
            List<Segment> segments = snapshot.getSegments();
            assertEquals("two flushed segments", 2, segments.size());

            long totalPrimaryRows = 0;
            long totalMvRows = 0;
            for (Segment seg : segments) {
                Map<String, WriterFileSet> byFormat = seg.dfGroupedSearchableFiles();
                assertTrue("segment " + seg.generation() + " has parquet", byFormat.containsKey("parquet"));
                assertTrue("segment " + seg.generation() + " has lucene", byFormat.containsKey("lucene"));
                assertTrue("segment " + seg.generation() + " has materialized_view", byFormat.containsKey(MVDataFormat.NAME));

                WriterFileSet mv = byFormat.get(MVDataFormat.NAME);
                assertEquals("one MV state file per segment", 1, mv.files().size());
                totalPrimaryRows += byFormat.get("parquet").numRows();
                totalMvRows += mv.numRows();

                // File physically exists in the mv dir
                Path mvFile = Path.of(mv.directory()).resolve(mv.files().iterator().next());
                assertTrue("state file exists: " + mvFile, Files.exists(mvFile));
            }
            assertEquals("8 raw docs across segments", 8, totalPrimaryRows);
            // seg1 has groups {api, web} = 2 rows; seg2 has {api, web, batch} = 3 rows
            assertEquals("5 state rows across segments", 5, totalMvRows);

            // ---- HARDCODED SEARCH: always goes to the MV state files ----
            java.util.List<String> stateFiles = new java.util.ArrayList<>();
            for (Segment seg : segments) {
                WriterFileSet mv = seg.dfGroupedSearchableFiles().get(MVDataFormat.NAME);
                for (String f : mv.files()) {
                    stateFiles.add(Path.of(mv.directory()).resolve(f).toString());
                }
            }
            String result = MVNativeBridge.search(stateFiles, MVConstants.GROUP_KEY, MVConstants.COUNT_STATE_COL);
            // Golden answers: api:5, web:2, batch:1 (sorted by service)
            assertEquals("api\t5\nbatch\t1\nweb\t2\n", result);
        }
    }

    private IndexShard getPrimaryShard(String indexName) {
        String nodeId = getClusterState().routingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        String nodeName = getClusterState().nodes().get(nodeId).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(indexName));
        return indexService.getShard(0);
    }
}
