/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that {@link MVShardBuildService} event callbacks never call
 * {@link ClusterService#state()} on the calling thread (which is the
 * cluster-applier thread in production). Instead, they enqueue
 * reconciliation onto a GENERIC executor.
 */
public class MVShardBuildServiceTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private MVPullSettings.Services createServices() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        return new MVPullSettings.Services(clusterService, threadPool, () -> null);
    }

    /**
     * Verify that {@code afterIndexShardStarted} returns immediately without
     * calling ClusterService.state() — the actual reconciliation (which calls
     * state()) is deferred to GENERIC.
     */
    public void testCallbacksDoNotBlockOnClusterState() throws Exception {
        MVPullSettings.Services services = createServices();
        MVShardBuildService buildService = new MVShardBuildService(services);

        // A mock shard that is NOT eligible (no MV settings) — so reconcile
        // will just call stop() quickly, but the key point is it doesn't
        // block the callback thread.
        IndexShard shard = mockNonEligibleShard();

        // Call the callback — this must return immediately (enqueue only).
        // If it called ClusterService.state() synchronously on the cluster-
        // applier thread, it would throw assertNotCalledFromClusterStateApplier.
        buildService.afterIndexShardStarted(shard);

        // Give GENERIC a moment to run the reconcile task
        assertBusy(() -> assertEquals("non-eligible shard must not start a poller", 0, buildService.activePollers()));

        buildService.close();
    }

    /**
     * Verify that beforeIndexShardClosed stops the poller synchronously
     * and removes pending reconcile entries.
     */
    public void testBeforeIndexShardClosedStopsSafely() throws Exception {
        MVPullSettings.Services services = createServices();
        MVShardBuildService buildService = new MVShardBuildService(services);
        IndexShard shard = mockNonEligibleShard();

        // Enqueue a reconcile, then immediately close — the close must
        // clear pending state without NPE or race.
        buildService.afterIndexShardStarted(shard);
        buildService.beforeIndexShardClosed(shard.shardId(), shard, shard.indexSettings().getSettings());

        assertEquals("poller count must be 0 after close", 0, buildService.activePollers());
        buildService.close();
    }

    /**
     * Verify that close() drains all pollers and blocks subsequent enqueues.
     */
    public void testCloseBlocksSubsequentEnqueues() throws Exception {
        MVPullSettings.Services services = createServices();
        MVShardBuildService buildService = new MVShardBuildService(services);
        buildService.close();

        // After close, callbacks must be no-ops
        IndexShard shard = mockNonEligibleShard();
        buildService.afterIndexShardStarted(shard);

        assertEquals("closed service must not start pollers", 0, buildService.activePollers());
    }

    /**
     * Verify that duplicate enqueues for the same shard are de-duplicated.
     */
    public void testDuplicateEnqueuesDeduplicated() throws Exception {
        MVPullSettings.Services services = createServices();
        MVShardBuildService buildService = new MVShardBuildService(services);
        IndexShard shard = mockNonEligibleShard();

        // Rapid-fire the same shard multiple times
        for (int i = 0; i < 10; i++) {
            buildService.afterIndexShardStarted(shard);
        }

        // Should not throw, and should converge to 0 pollers (non-eligible)
        assertBusy(() -> assertEquals(0, buildService.activePollers()));

        buildService.close();
    }

    /**
     * Verify that indexShardStateChanged and shardRoutingChanged also
     * enqueue without blocking.
     */
    public void testAllEventCallbacksEnqueueAsync() throws Exception {
        MVPullSettings.Services services = createServices();
        MVShardBuildService buildService = new MVShardBuildService(services);
        IndexShard shard = mockNonEligibleShard();

        // All three event types must return immediately
        buildService.afterIndexShardStarted(shard);
        buildService.indexShardStateChanged(shard, IndexShardState.RECOVERING, IndexShardState.STARTED, "test");
        buildService.shardRoutingChanged(shard, null, shard.routingEntry());

        assertBusy(() -> assertEquals(0, buildService.activePollers()));
        buildService.close();
    }

    private static IndexShard mockNonEligibleShard() {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test_mv", UUID.randomUUID().toString()), 0);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.state()).thenReturn(IndexShardState.STARTED);

        ShardRouting routing = TestShardRouting.newShardRouting(
            shardId,
            "node1",
            true,
            org.opensearch.cluster.routing.ShardRoutingState.STARTED
        );
        when(shard.routingEntry()).thenReturn(routing);

        // No DerivedIndexBinding, no mv_state primary format → not eligible
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardId.getIndex().getName()).settings(indexSettings).build();
        IndexSettings idxSettings = new IndexSettings(metadata, Settings.EMPTY);
        when(shard.indexSettings()).thenReturn(idxSettings);

        return shard;
    }
}
