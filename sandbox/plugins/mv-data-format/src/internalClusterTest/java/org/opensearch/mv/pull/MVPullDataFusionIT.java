/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.mv.MVDataFormatPlugin;
import org.opensearch.mv.MVNativeBridge;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * The PRODUCT read path (un-skewing gate): the source is a COMPOSITE index
 * (parquet primary carrying the {@code _seq_no} column), remote-backed; the
 * MV pulls the published generation and folds the delta THROUGH DATAFUSION
 * ({@code MVNativeBridge.buildArrow}), not Lucene search. Verifies exact
 * per-group equality across multiple published generations.
 */
@ThreadLeakFilters(filters = MVPullDataFusionIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class MVPullDataFusionIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String SOURCE = "hits_src_df";
    private static final String MV = "hits_mv_df";
    private static final String REPO = "mv-pull-df-repo";
    private static final String FINAL_SQL = "SELECT \"RegionID\", "
        + "SUM(\"count(Int64(1))[count]\") AS cnt, "
        + "SUM(\"sum(mv_input.AdvEngineID)[sum]\") AS adv "
        + "FROM __MV_STATES__ GROUP BY \"RegionID\" ORDER BY \"RegionID\"";

    private Path repoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            FlightStreamPlugin.class,
            AnalyticsPlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (repoPath == null) {
            repoPath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .put(remoteStoreClusterSettings(REPO, repoPath))
            .build();
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testDataFusionFoldedPullMatchesSource() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(SOURCE)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put("index.refresh_interval", "10s")
                    .put("index.derived.enabled", false)
            )
            .setMapping("RegionID", "type=long", "AdvEngineID", "type=long")
            .get();
        ensureGreen(SOURCE);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("cluster.pluggable.dataformat", "composite"))
            .get();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(MV)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put("index.pluggable.dataformat.enabled", true)
                        .put("index.pluggable.dataformat", "composite")
                        .put("index.composite.primary_data_format", MVStateDataFormat.NAME)
                        .putList("index.composite.secondary_data_formats")
                        .put("index.derived.enabled", true)
                        .put("index.mv.definition", "pull_count_sum")
                        .putList("index.mv.state_fields", "RegionID", "cnt", "adv")
                        .put("index.mv.state_merge_enabled", true)
                        .put(MVPullSettings.PULL_INTERVAL.getKey(), "100ms")
                        .put("index.mv.serve_state", true)
                        // Public settings only; MetadataCreateIndexService enriches private binding
                        // (UUID, topology, mapping mode). The deprecated index.mv_pull.source_index
                        // is NOT set — all pull targets use DerivedIndexBinding exclusively.
                        .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_SOURCE_NAME, SOURCE)
                        .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "pull_count_sum")
                )
                .setMapping("RegionID", "type=long", "cnt", "type=long", "adv", "type=long")
        );
        ensureGreen(MV);
        assertBusy(this::assertPrimaryOnlyBuildService);

        Map<Long, long[]> expected = new HashMap<>();
        int docs = 0;
        for (int generation = 0; generation < 3; generation++) {
            int batch = randomIntBetween(15, 40);
            for (int i = 0; i < batch; i++) {
                long region = randomIntBetween(0, 7);
                long adv = randomIntBetween(0, 5);
                client().prepareIndex(SOURCE).setSource("RegionID", region, "AdvEngineID", adv).get();
                expected.computeIfAbsent(region, r -> new long[2]);
                expected.get(region)[0] += 1;
                expected.get(region)[1] += adv;
                docs++;
            }
            client().admin().indices().prepareRefresh(SOURCE).get();
        }
        final int totalDocs = docs;

        assertBusy(() -> assertStateFilesEqual(expected, totalDocs), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, totalDocs), 60, java.util.concurrent.TimeUnit.SECONDS);

        // Operation-free durability: the derived target publishes catalog artifacts,
        // never source-operation translog entries.
        var translogStats = client().admin().indices().prepareStats(MV).setTranslog(true).get().getPrimaries().getTranslog();
        assertEquals("MV translog must be operation-free", 0, translogStats.estimatedNumberOfOperations());

        // Continued pulling across a later source checkpoint appends another range-state artifact.
        final long coveredSoFar = totalDocs;
        for (int i = 0; i < 12; i++) {
            long region = i % 5;
            client().prepareIndex(SOURCE).setSource("RegionID", region, "AdvEngineID", 1).get();
            expected.computeIfAbsent(region, r -> new long[2]);
            expected.get(region)[0] += 1;
            expected.get(region)[1] += 1;
        }
        client().admin().indices().prepareRefresh(SOURCE).get();
        assertBusy(() -> assertStateFilesEqual(expected, coveredSoFar + 12), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, coveredSoFar + 12), 60, java.util.concurrent.TimeUnit.SECONDS);

        // Deterministic primary relocation: exclude the current MV primary node so the
        // build service must hand off without also killing a co-located source primary.
        // Hard process loss remains part of the A/B chaos gate.
        String failedPrimaryNode = primaryNodeName(MV);
        long watermarkBeforeFailover = publishedState(failedPrimaryNode).watermark().seqNo();
        String relocationTarget = spareDataNodeName();
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand(MV, 0, failedPrimaryNode, relocationTarget))
            .get();
        assertBusy(
            () -> assertNotEquals("MV primary must move to the relocation target", failedPrimaryNode, primaryNodeName(MV)),
            60,
            java.util.concurrent.TimeUnit.SECONDS
        );
        ensureGreen(SOURCE, MV);
        assertBusy(this::assertPrimaryOnlyBuildService);
        assertEquals(watermarkBeforeFailover, publishedState(primaryNodeName(MV)).watermark().seqNo());

        final long beforePostFailover = coveredSoFar + 12;
        for (int i = 0; i < 9; i++) {
            long region = i % 4;
            client().prepareIndex(SOURCE).setSource("RegionID", region, "AdvEngineID", 2).get();
            expected.computeIfAbsent(region, r -> new long[2]);
            expected.get(region)[0] += 1;
            expected.get(region)[1] += 2;
        }
        client().admin().indices().prepareRefresh(SOURCE).get();
        assertBusy(() -> assertStateFilesEqual(expected, beforePostFailover + 9), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, beforePostFailover + 9), 60, java.util.concurrent.TimeUnit.SECONDS);

        // Process-level durability: stop every node, reopen both remote-backed indices,
        // recover W from the MV CatalogSnapshot, and recreate exactly one primary poller.
        long watermarkBeforeRestart = publishedState(primaryNodeName(MV)).watermark().seqNo();
        int nodesBeforeRestart = internalCluster().size();
        internalCluster().fullRestart();
        ensureStableCluster(nodesBeforeRestart);
        ensureGreen(SOURCE, MV);
        assertBusy(this::assertPrimaryOnlyBuildService, 60, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(
            "durable MV watermark must survive full restart",
            watermarkBeforeRestart,
            publishedState(primaryNodeName(MV)).watermark().seqNo()
        );
        assertBusy(() -> assertStateFilesEqual(expected, beforePostFailover + 9), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, beforePostFailover + 9), 60, java.util.concurrent.TimeUnit.SECONDS);

        // The reconstructed poller must continue from recovered W without replaying or
        // skipping the next explicitly published source generation.
        final long beforePostRestart = beforePostFailover + 9;
        for (int i = 0; i < 7; i++) {
            long region = i % 3;
            client().prepareIndex(SOURCE).setSource("RegionID", region, "AdvEngineID", 3).get();
            expected.computeIfAbsent(region, r -> new long[2]);
            expected.get(region)[0] += 1;
            expected.get(region)[1] += 3;
        }
        client().admin().indices().prepareRefresh(SOURCE).get();
        assertBusy(() -> assertStateFilesEqual(expected, beforePostRestart + 7), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, beforePostRestart + 7), 60, java.util.concurrent.TimeUnit.SECONDS);

        // Compact immutable per-range files through the certified DataFusion state fold.
        // The catalog switch must preserve W and exact answers, and releasing the old
        // snapshot must reclaim every superseded Arrow artifact.
        PublishedState beforeCompaction = publishedState(primaryNodeName(MV));
        assertTrue("test must publish multiple immutable ranges before compaction", beforeCompaction.files().size() > 1);
        for (int compactionPass = 0; compactionPass < 4 && publishedState(primaryNodeName(MV)).files().size() > 1; compactionPass++) {
            var forceMerge = client().admin().indices().prepareForceMerge(MV).setMaxNumSegments(1).setFlush(true).get();
            assertEquals("mv_state compaction must not fail a shard", 0, forceMerge.getFailedShards());
        }
        assertBusy(
            () -> assertEquals("compaction must publish one mv_state file", 1, publishedState(primaryNodeName(MV)).files().size()),
            60,
            java.util.concurrent.TimeUnit.SECONDS
        );
        assertEquals("compaction must not advance W", beforeCompaction.watermark(), publishedState(primaryNodeName(MV)).watermark());
        assertBusy(() -> assertStateFilesEqual(expected, beforePostRestart + 7), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> assertReplicaStateEqual(expected, beforePostRestart + 7), 60, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> {
            for (String superseded : beforeCompaction.files()) {
                assertFalse("superseded mv_state artifact must be deleted: " + superseded, java.nio.file.Files.exists(Path.of(superseded)));
            }
        }, 60, java.util.concurrent.TimeUnit.SECONDS);
    }

    private void assertStateFilesEqual(Map<Long, long[]> expected, long expectedDocs) throws Exception {
        assertPublishedStateEqual(publishedState(primaryNodeName(MV)), expected, expectedDocs);
    }

    private void assertReplicaStateEqual(Map<Long, long[]> expected, long expectedDocs) throws Exception {
        String replicaNode = replicaNodeName();
        PublishedState primary = publishedState(primaryNodeName(MV));
        PublishedState replica = publishedState(replicaNode);
        assertEquals("replica watermark must match primary", primary.watermark(), replica.watermark());
        assertPublishedStateEqual(replica, expected, expectedDocs);
    }

    private void assertPublishedStateEqual(PublishedState published, Map<Long, long[]> expected, long expectedDocs) {
        java.util.List<String> stateFiles = published.files();
        assertFalse("MV catalog must contain mv_state files", stateFiles.isEmpty());
        assertEquals("catalog watermark must cover exactly the source prefix", expectedDocs - 1L, published.watermark().seqNo());
        String output = MVNativeBridge.searchV2(stateFiles, FINAL_SQL);
        Map<Long, long[]> actual = new HashMap<>();
        long total = 0L;
        for (String line : output.lines().filter(value -> value.isBlank() == false).toList()) {
            String[] cells = line.split("\\t");
            assertEquals("unexpected final state row: " + line, 3, cells.length);
            long region = Long.parseLong(cells[0]);
            long count = Long.parseLong(cells[1]);
            long sum = Long.parseLong(cells[2]);
            actual.put(region, new long[] { count, sum });
            total += count;
        }
        assertEquals("MV must cover every source doc", expectedDocs, total);
        assertEquals("group cardinality", expected.size(), actual.size());
        for (Map.Entry<Long, long[]> entry : expected.entrySet()) {
            long[] got = actual.get(entry.getKey());
            assertNotNull("missing MV group " + entry.getKey(), got);
            assertArrayEquals("state for region " + entry.getKey(), entry.getValue(), got);
        }
    }

    private void assertPrimaryOnlyBuildService() {
        String primaryNode = primaryNodeName(MV);
        String replicaNode = replicaNodeName();
        assertEquals("MV primary must run one poller", 1, pullService(primaryNode).activePollers());
        assertEquals("MV replica must not run a poller", 0, pullService(replicaNode).activePollers());
    }

    private org.opensearch.index.engine.derived.pull.NodeDerivedPullService pullService(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName).filterPlugins(MVDataFormatPlugin.class).get(0).pullService();
    }

    private String spareDataNodeName() {
        String primary = primaryNodeName(MV);
        String replica = replicaNodeName();
        return internalCluster().getDataNodeNames()
            .stream()
            .filter(node -> node.equals(primary) == false && node.equals(replica) == false)
            .findFirst()
            .orElseThrow();
    }

    private String replicaNodeName() {
        var replica = getClusterState().routingTable()
            .index(MV)
            .shard(0)
            .replicaShards()
            .stream()
            .filter(org.opensearch.cluster.routing.ShardRouting::assignedToNode)
            .findFirst()
            .orElseThrow();
        return getClusterState().nodes().get(replica.currentNodeId()).getName();
    }

    private PublishedState publishedState(String nodeName) throws Exception {
        org.opensearch.indices.IndicesService indicesService = internalCluster().getInstance(
            org.opensearch.indices.IndicesService.class,
            nodeName
        );
        org.opensearch.index.shard.IndexShard shard = indicesService.indexServiceSafe(getClusterState().metadata().index(MV).getIndex())
            .getShard(0);
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = shard
                .getCatalogSnapshot()
        ) {
            java.util.List<String> files = ref.get()
                .getSearchableFiles(MVStateDataFormat.NAME)
                .stream()
                .flatMap(fileSet -> fileSet.files().stream().map(file -> Path.of(fileSet.directory()).resolve(file).toString()))
                .sorted()
                .toList();
            String encoded = ref.get().getUserData().get(MVWatermark.key(0));
            assertNotNull("MV catalog must contain watermark", encoded);
            return new PublishedState(files, MVWatermark.decode(encoded));
        }
    }

    private record PublishedState(java.util.List<String> files, MVWatermark watermark) {
    }
}
