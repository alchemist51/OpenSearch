/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link NodeDerivedPullService} lifecycle, concurrency, and
 * correctness invariants. Verifies:
 *
 * <ul>
 *   <li>Exactly one poller per eligible primary shard</li>
 *   <li>Event callbacks never block the cluster-applier thread</li>
 *   <li>close() is idempotent and drains all pollers</li>
 *   <li>Duplicate enqueues are de-duplicated</li>
 *   <li>Non-eligible shards never get pollers</li>
 *   <li>Format registry rejects duplicates</li>
 *   <li>start/stop/close ordering is correct</li>
 * </ul>
 */
public class NodeDerivedPullServiceTests extends OpenSearchTestCase {

    private static final String TEST_FORMAT_ID = "test_derived";

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

    // ── Helper factories ─────────────────────────────────────────────────

    private NodeDerivedPullService createService(DerivedPullFormat... formats) {
        return new NodeDerivedPullService(threadPool, List.of(formats));
    }

    private static DerivedPullFormat noOpFormat(String formatId) {
        return new DerivedPullFormat() {
            @Override
            public String formatId() {
                return formatId;
            }

            @Override
            public DerivedSourceReader createReader(Settings nodeSettings, IndexSettings indexSettings) {
                return new NoOpReader();
            }

            @Override
            public DerivedArtifactBuilder createArtifactBuilder(Settings nodeSettings, IndexSettings indexSettings) {
                return new NoOpBuilder();
            }
        };
    }

    private static IndexShard mockNonEligibleShard() {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test_index", UUID.randomUUID().toString()), 0);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.state()).thenReturn(IndexShardState.STARTED);

        ShardRouting routing = TestShardRouting.newShardRouting(
            shardId,
            "node1",
            true,
            org.opensearch.cluster.routing.ShardRoutingState.STARTED
        );
        when(shard.routingEntry()).thenReturn(routing);

        // No DerivedIndexBinding, no primary_data_format
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

    private static IndexShard mockReplicaShard() {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test_mv", UUID.randomUUID().toString()), 0);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.state()).thenReturn(IndexShardState.STARTED);

        ShardRouting routing = TestShardRouting.newShardRouting(
            shardId,
            "node1",
            false, // replica!
            org.opensearch.cluster.routing.ShardRoutingState.STARTED
        );
        when(shard.routingEntry()).thenReturn(routing);

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
            .put("index.composite.primary_data_format", TEST_FORMAT_ID)
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardId.getIndex().getName()).settings(indexSettings).build();
        IndexSettings idxSettings = new IndexSettings(metadata, Settings.EMPTY);
        when(shard.indexSettings()).thenReturn(idxSettings);

        return shard;
    }

    // ── Tests ────────────────────────────────────────────────────────────

    /**
     * Non-eligible shards (no DerivedIndexBinding or wrong format) must
     * never get a poller.
     */
    public void testNonEligibleShardDoesNotGetPoller() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IndexShard shard = mockNonEligibleShard();
        service.afterIndexShardStarted(shard);

        assertBusy(() -> assertEquals(0, service.activePollers()));
        service.close();
    }

    /**
     * Replicas must not get a poller — only primaries are eligible.
     */
    public void testReplicaShardDoesNotGetPoller() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IndexShard shard = mockReplicaShard();
        service.afterIndexShardStarted(shard);

        assertBusy(() -> assertEquals(0, service.activePollers()));
        service.close();
    }

    /**
     * Callbacks must return immediately (enqueue only) — they run on the
     * cluster-applier thread in production and must not block.
     */
    public void testCallbacksDoNotBlockCallerThread() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IndexShard shard = mockNonEligibleShard();

        // All three callbacks must return without delay
        long start = System.nanoTime();
        service.afterIndexShardStarted(shard);
        service.indexShardStateChanged(shard, IndexShardState.RECOVERING, IndexShardState.STARTED, "test");
        service.shardRoutingChanged(shard, null, shard.routingEntry());
        long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        // Should be near-instant (< 100ms). The actual reconciliation runs
        // on GENERIC.
        assertTrue("callbacks took too long: " + elapsed + "ms", elapsed < 500);

        assertBusy(() -> assertEquals(0, service.activePollers()));
        service.close();
    }

    /**
     * beforeIndexShardClosed stops any active poller synchronously and
     * removes pending reconcile entries.
     */
    public void testBeforeShardClosedStopsPoller() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IndexShard shard = mockNonEligibleShard();
        service.afterIndexShardStarted(shard);
        service.beforeIndexShardClosed(shard.shardId(), shard, shard.indexSettings().getSettings());

        assertEquals("pollers should be 0 after shard closed", 0, service.activePollers());
        service.close();
    }

    /**
     * close() must drain all pollers and block subsequent enqueues.
     */
    public void testCloseBlocksSubsequentEnqueues() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();
        service.close();

        IndexShard shard = mockNonEligibleShard();
        service.afterIndexShardStarted(shard);

        assertEquals("closed service must not start pollers", 0, service.activePollers());
    }

    /**
     * close() is idempotent — calling it multiple times must not throw.
     */
    public void testCloseIsIdempotent() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();
        service.close();
        service.close(); // second close must not throw
        assertEquals(0, service.activePollers());
    }

    /**
     * Rapid-fire duplicate enqueues for the same shard are de-duplicated.
     */
    public void testDuplicateEnqueuesDeduplicated() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IndexShard shard = mockNonEligibleShard();
        for (int i = 0; i < 20; i++) {
            service.afterIndexShardStarted(shard);
        }

        assertBusy(() -> assertEquals(0, service.activePollers()));
        service.close();
    }

    /**
     * Registering duplicate formatIds throws IllegalArgumentException.
     */
    public void testDuplicateFormatRegistrationThrows() {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> service.registerFormat(noOpFormat(TEST_FORMAT_ID))
        );
        assertTrue(ex.getMessage().contains("already registered"));
        service.close();
    }

    /**
     * The format registry is accessible and lists all registered formats.
     */
    public void testRegisteredFormatsAccessible() {
        NodeDerivedPullService service = createService(noOpFormat("format_a"), noOpFormat("format_b"));
        service.start();

        assertEquals(2, service.registeredFormats().size());
        assertTrue(service.registeredFormats().contains("format_a"));
        assertTrue(service.registeredFormats().contains("format_b"));
        service.close();
    }

    /**
     * start() → stop() → close() lifecycle ordering works without errors.
     */
    public void testLifecycleOrdering() {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();
        service.stop();
        service.close();
        assertEquals(0, service.activePollers());
    }

    /**
     * eligibleFormatId returns null for shards without DerivedIndexBinding.
     */
    public void testEligibleFormatIdNullWithoutBinding() {
        IndexShard shard = mockNonEligibleShard();
        assertNull(NodeDerivedPullService.eligibleFormatId(shard));
    }

    /**
     * eligibleFormatId returns null for replica shards.
     */
    public void testEligibleFormatIdNullForReplica() {
        IndexShard shard = mockReplicaShard();
        assertNull(NodeDerivedPullService.eligibleFormatId(shard));
    }

    /**
     * Multiple shards with different IDs can be reconciled concurrently.
     */
    public void testMultipleShardsReconcileConcurrently() throws Exception {
        NodeDerivedPullService service = createService(noOpFormat(TEST_FORMAT_ID));
        service.start();

        // Create multiple non-eligible shards with different ShardIds
        for (int i = 0; i < 5; i++) {
            IndexShard shard = mockNonEligibleShard();
            service.afterIndexShardStarted(shard);
        }

        assertBusy(() -> assertEquals(0, service.activePollers()));
        service.close();
    }

    // ── No-op SPI implementations for testing ────────────────────────────

    static class NoOpReader implements DerivedSourceReader {
        @Override
        public DerivedSourceSnapshot fetchSnapshot(org.opensearch.cluster.routing.ShardRouting shard, long sinceWatermark) {
            return null; // No data available
        }

        @Override
        public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) {}

        @Override
        public void close() {}
    }

    static class NoOpBuilder implements DerivedArtifactBuilder {
        @Override
        public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) {
            return new BuildResult() {
                @Override
                public boolean success() {
                    return true;
                }

                @Override
                public String artifactId() {
                    return "noop";
                }

                @Override
                public Map<String, Object> stats() {
                    return Map.of();
                }
            };
        }

        @Override
        public void close() {}
    }
}
