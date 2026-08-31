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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
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
 * Source-driven target commit contract: a source commit returns without
 * waiting for target durability, then asynchronously advances the target's
 * cap and forces a target commit. The target may persist only the exact
 * published claim at or below that source checkpoint.
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

    public void testSourceCommitAsynchronouslyCapsAndCommitsTarget() throws Exception {
        // Explicit pair creation keeps this test focused on commit signaling
        // rather than target auto-creation.
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
                    .put(MVConstants.DERIVED_INDEX_SETTING, true)
                    .put("index.append_only.enabled", true)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "mv_state")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(MVViewsService.TargetCreator.targetMapping("payments"))
            .get();
        ensureGreen(TARGET);

        // Two generations shipped (refresh per batch).
        client().prepareIndex(SOURCE).setSource("service", "api", "status", "200", "latency_ms", 30).get();
        client().admin().indices().prepareRefresh(SOURCE).get();
        client().prepareIndex(SOURCE).setSource("service", "web", "status", "500", "latency_ms", 80).get();
        client().admin().indices().prepareRefresh(SOURCE).get();

        String targetNode = getClusterState().nodes()
            .get(getClusterState().routingTable().index(TARGET).shard(0).primaryShard().currentNodeId())
            .getName();
        org.opensearch.transport.client.Client targetClient = internalCluster().client(targetNode);
        MVCursorAction.Request cursorRequest = new MVCursorAction.Request(TARGET, 0, SOURCE, 0);
        MVCursorAction.Response beforeTargetCommit = targetClient.execute(MVCursorAction.INSTANCE, cursorRequest).actionGet();
        assertTrue("live applied cursor should advance at searchable ack", beforeTargetCommit.checkpoint() >= 1L);

        IndicesService indicesService = internalCluster().getDataNodeInstance(IndicesService.class);
        IndexShard targetShard = indicesService.indexServiceSafe(resolveIndex(TARGET)).getShard(0);
        Map<String, String> targetBeforeCommit = targetShard.store().readLastCommittedSegmentsInfo().getUserData();
        assertFalse(
            "live cursor must not appear in durable metadata before target commit",
            targetBeforeCommit.containsKey(MVConstants.CURSOR_KEY_PREFIX + SOURCE + ".0")
        );

        // A source no-op consumes seq-no 2 but produces no parquet row. The
        // next source refresh must ship it as a zero-row exact-coverage batch,
        // advancing the target claim without adding a target document.
        IndexShard sourceShard = indicesService.indexServiceSafe(resolveIndex(SOURCE)).getShard(0);
        org.opensearch.index.shard.MVNoOpTestHelper.markPrimaryNoOp(sourceShard, 2L, "mv no-op announcement test");
        client().admin().indices().prepareRefresh(SOURCE).get();
        MVCursorAction.Response afterNoOp = targetClient.execute(MVCursorAction.INSTANCE, cursorRequest).actionGet();
        assertEquals(2L, afterNoOp.sourceCoverage().floor());
        assertTrue(afterNoOp.sourceCoverage().aboveFloor().isEmpty());
        try (var targetSnapshot = targetShard.getCatalogSnapshot()) {
            assertEquals(2L, targetSnapshot.get().getNumDocs());
        }

        // Source commit contains no target watermark and does not wait for the
        // target action. The asynchronous handler advances the cap and forces
        // the eligible target catalog commit afterward.
        assertEquals(0, client().admin().indices().prepareFlush(SOURCE).get().getFailedShards());
        Map<String, String> sourceCommit = sourceShard.store().readLastCommittedSegmentsInfo().getUserData();
        assertFalse(
            "source commit must contain no target durability watermark: " + sourceCommit.keySet(),
            sourceCommit.keySet().stream().anyMatch(k -> k.startsWith("mv.commit."))
        );
        long sourceCommittedCheckpoint = Long.parseLong(sourceCommit.get(org.opensearch.index.seqno.SequenceNumbers.LOCAL_CHECKPOINT_KEY));

        String encodedNoOps = sourceCommit.get(MVConstants.SOURCE_NOOP_COVERAGE_KEY);
        assertNotNull("source commit must retain exact no-op coverage", encodedNoOps);
        MVSourceSeqCoverage durableNoOps = MVSourceSeqCoverage.decode(encodedNoOps);
        assertTrue(durableNoOps.contains(2L));
        assertTrue(durableNoOps.maxClaimedSeqNo() <= sourceCommittedCheckpoint);

        assertBusy(() -> {
            Map<String, String> targetCommit = targetShard.store().readLastCommittedSegmentsInfo().getUserData();
            String encoded = targetCommit.get(MVConstants.CURSOR_KEY_PREFIX + SOURCE + ".0");
            assertNotNull("asynchronous target commit has not published its source cursor; keys=" + targetCommit.keySet(), encoded);
            MVTargetCursorLedger.Cursor durableCursor = MVTargetCursorLedger.Cursor.decode(encoded);
            MVSourceSeqCoverage durableCoverage = MVTargetCursorLedger.decodeCommitCoverage(encoded);
            assertTrue(durableCursor.certifiedGeneration() >= 1L);
            assertTrue(durableCoverage.maxClaimedSeqNo() <= sourceCommittedCheckpoint);
            assertEquals(2L, durableCoverage.floor());
            assertTrue(durableCoverage.aboveFloor().isEmpty());

            long targetMaxSeqNo = Long.parseLong(targetCommit.get(org.opensearch.index.seqno.SequenceNumbers.MAX_SEQ_NO));
            String targetTranslogUuid = targetCommit.get(Translog.TRANSLOG_UUID_KEY);
            long persistedGlobalCheckpoint = Translog.readGlobalCheckpoint(targetShard.shardPath().resolveTranslog(), targetTranslogUuid);
            assertTrue(
                "target commit max sequence number "
                    + targetMaxSeqNo
                    + " is newer than its persisted translog global checkpoint "
                    + persistedGlobalCheckpoint,
                persistedGlobalCheckpoint >= targetMaxSeqNo
            );
            assertEquals(
                "derived target translog must remain operation-free",
                0,
                targetShard.translogStats().estimatedNumberOfOperations()
            );
        });

        MVCursorAction.Response afterTargetCommit = targetClient.execute(MVCursorAction.INSTANCE, cursorRequest).actionGet();
        assertTrue("target cursor must remain certified", afterTargetCommit.certifiedGeneration() >= 1L);
        assertEquals(2L, afterTargetCommit.checkpoint());
        assertEquals(MVSourceSeqCoverage.contiguous(2L), afterTargetCommit.sourceCoverage());
    }
}
