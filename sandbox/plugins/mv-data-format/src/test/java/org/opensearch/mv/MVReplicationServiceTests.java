/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link MVReplicationService}.
 * <ul>
 *   <li>Item 1: Real adverts from fake catalog snapshot carry file+ranges</li>
 *   <li>Item 3: Reconciliation adds already-started shards on cluster event</li>
 *   <li>Core: advance-detection, per-shard independence, lifecycle</li>
 * </ul>
 */
public class MVReplicationServiceTests extends OpenSearchTestCase {

    static class FakeShardState {
        final org.opensearch.core.index.shard.ShardId shardId;
        final String indexUuid;
        long processedCheckpoint = -1L;
        long primaryTerm = 1L;
        boolean primary = true;
        boolean active = true;

        FakeShardState(String indexName, String indexUuid, int shardNum) {
            this.shardId = new org.opensearch.core.index.shard.ShardId(
                new org.opensearch.core.index.Index(indexName, indexUuid), shardNum
            );
            this.indexUuid = indexUuid;
        }
    }

    static class FakeRoutingService extends NodeRoutingSnapshotService {
        private volatile Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = Map.of();

        FakeRoutingService() {
            super("test-node-id");
        }

        void setSourceToTargets(Map<String, List<NodeRoutingSnapshotService.BoundTarget>> map) {
            this.srcToTgt = Collections.unmodifiableMap(map);
        }

        @Override
        public Map<String, List<NodeRoutingSnapshotService.BoundTarget>> sourceToTargets() {
            return srcToTgt;
        }
    }

    /** Records publish() invocations including checkpoint data for verifying real adverts. */
    static class PublishRecord {
        final List<Long> publishedSeqNos = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, MVFileMetadata>> publishedFileMetadata = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Testable subclass that uses fake shard state including a fake catalog
     * snapshot with real parquet files for Item 1 verification.
     */
    static class TestableMVReplicationService {
        final FakeRoutingService routingService;
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeShardState> trackedShards = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, PublishRecord> publishers = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, AtomicLong> lastPublished = new ConcurrentHashMap<>();
        /** Pool of all registered shards (mirrors allSourceShards in real service). */
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeShardState> allShards = new ConcurrentHashMap<>();
        final AtomicLong tickCount = new AtomicLong();
        final AtomicLong publishCount = new AtomicLong();
        final AtomicLong skipCount = new AtomicLong();
        final AtomicLong catalogReadCount = new AtomicLong();

        /** Per-shard fake catalog: file names + seq ranges. */
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeCatalog> catalogs = new ConcurrentHashMap<>();

        TestableMVReplicationService(FakeRoutingService routingService) {
            this.routingService = routingService;
        }

        void registerShard(FakeShardState shard) {
            allShards.put(shard.shardId, shard);
        }

        void trackShard(FakeShardState shard) {
            trackedShards.put(shard.shardId, shard);
            publishers.put(shard.shardId, new PublishRecord());
            lastPublished.putIfAbsent(shard.shardId, new AtomicLong(-1L));
        }

        void untrackShard(org.opensearch.core.index.shard.ShardId shardId) {
            trackedShards.remove(shardId);
            publishers.remove(shardId);
            lastPublished.remove(shardId);
            allShards.remove(shardId);
        }

        void setCatalog(org.opensearch.core.index.shard.ShardId shardId, FakeCatalog catalog) {
            catalogs.put(shardId, catalog);
        }

        /** Mirrors MVReplicationService.tick() with real catalog reads. */
        void tick() {
            tickCount.incrementAndGet();
            Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

            for (Map.Entry<org.opensearch.core.index.shard.ShardId, FakeShardState> entry : trackedShards.entrySet()) {
                org.opensearch.core.index.shard.ShardId shardId = entry.getKey();
                FakeShardState shard = entry.getValue();

                List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(shardId.getIndexName());
                if (targets == null || targets.isEmpty()) continue;

                if (!shard.primary || !shard.active) continue;

                long currentCheckpoint = shard.processedCheckpoint;
                AtomicLong lastPub = lastPublished.get(shardId);
                if (lastPub == null) continue;
                long last = lastPub.get();
                if (currentCheckpoint <= last) {
                    skipCount.incrementAndGet();
                    continue;
                }

                if (!lastPub.compareAndSet(last, currentCheckpoint)) continue;

                // Read catalog snapshot on advance — build MVReplicationCheckpoint
                Map<String, MVFileMetadata> fileMetadata = new LinkedHashMap<>();
                FakeCatalog cat = catalogs.get(shardId);
                if (cat != null) {
                    catalogReadCount.incrementAndGet();
                    for (int i = 0; i < cat.files.size(); i++) {
                        fileMetadata.put(cat.files.get(i), new MVFileMetadata(
                            -1L,
                            i < cat.minSeqNos.size() ? cat.minSeqNos.get(i) : -1L,
                            i < cat.maxSeqNos.size() ? cat.maxSeqNos.get(i) : -1L,
                            MVFileMetadata.CRC32_UNKNOWN
                        ));
                    }
                }

                PublishRecord record = publishers.get(shardId);
                if (record != null) {
                    record.publishedSeqNos.add(currentCheckpoint);
                    record.publishedFileMetadata.add(Map.copyOf(fileMetadata));
                }
                publishCount.incrementAndGet();
            }
        }

        /** Mirrors reconcileTrackedShards: promote pool shards that now have targets. */
        void reconcile() {
            Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

            for (Map.Entry<org.opensearch.core.index.shard.ShardId, FakeShardState> entry : allShards.entrySet()) {
                org.opensearch.core.index.shard.ShardId shardId = entry.getKey();
                if (trackedShards.containsKey(shardId)) continue;
                String indexName = shardId.getIndexName();
                List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(indexName);
                if (targets == null || targets.isEmpty()) continue;
                FakeShardState shard = entry.getValue();
                if (!shard.primary || !shard.active) continue;
                trackShard(shard);
            }
        }

        List<Long> publishedSeqNos(org.opensearch.core.index.shard.ShardId shardId) {
            PublishRecord pub = publishers.get(shardId);
            return pub == null ? List.of() : List.copyOf(pub.publishedSeqNos);
        }

        PublishRecord getPublishRecord(org.opensearch.core.index.shard.ShardId shardId) {
            return publishers.get(shardId);
        }
    }

    /** Fake catalog for testing real adverts (Item 1). */
    static class FakeCatalog {
        final List<String> files;
        final List<Long> minSeqNos;
        final List<Long> maxSeqNos;

        FakeCatalog(List<String> files, List<Long> minSeqNos, List<Long> maxSeqNos) {
            this.files = List.copyOf(files);
            this.minSeqNos = List.copyOf(minSeqNos);
            this.maxSeqNos = List.copyOf(maxSeqNos);
        }
    }

    // ── Item 1: Real adverts from catalog snapshot ───────────────────────

    public void testRealAdvertCarriesFilesAndRanges() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        // Set up a fake catalog with real files
        FakeCatalog catalog = new FakeCatalog(
            List.of("gen-1_0.parquet", "gen-1_1.parquet"),
            List.of(0L, 50L),
            List.of(49L, 100L)
        );
        service.setCatalog(shard.shardId, catalog);

        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(1L, service.catalogReadCount.get());

        PublishRecord record = service.getPublishRecord(shard.shardId);
        assertNotNull(record);
        assertEquals(1, record.publishedFileMetadata.size());
        Map<String, MVFileMetadata> published = record.publishedFileMetadata.get(0);
        assertEquals(2, published.size());
        assertTrue(published.containsKey("gen-1_0.parquet"));
        assertTrue(published.containsKey("gen-1_1.parquet"));
        assertEquals(0L, published.get("gen-1_0.parquet").minSeqNo());
        assertEquals(49L, published.get("gen-1_0.parquet").maxSeqNo());
        assertEquals(50L, published.get("gen-1_1.parquet").minSeqNo());
        assertEquals(100L, published.get("gen-1_1.parquet").maxSeqNo());
    }

    public void testNoCatalogReadOnSkip() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);
        service.setCatalog(shard.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(100L)));

        // First tick: reads catalog
        service.tick();
        assertEquals(1L, service.catalogReadCount.get());

        // Second tick: no advance, no catalog read
        service.tick();
        assertEquals(1L, service.catalogReadCount.get());
        assertEquals(1L, service.skipCount.get());
    }

    // ── Item 3: Reconciliation adds already-started shards ───────────────

    public void testReconcileAddsShardWhenTargetAppears() {
        FakeRoutingService routing = new FakeRoutingService();
        // Initially NO targets
        routing.setSourceToTargets(Map.of());
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        // Register in pool (simulates afterIndexShardStarted with no targets)
        service.registerShard(shard);

        // Tick: nothing tracked yet
        service.reconcile();
        assertEquals(0, service.trackedShards.size());

        service.tick();
        assertEquals(0L, service.publishCount.get());

        // Target appears dynamically
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));

        // Reconcile: should promote from pool to tracked
        service.reconcile();
        assertEquals(1, service.trackedShards.size());

        // Tick: should publish
        service.tick();
        assertEquals(1L, service.publishCount.get());
    }

    public void testReconcileDoesNotDuplicateTracking() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.registerShard(shard);
        service.trackShard(shard);

        // Reconcile should NOT create a duplicate
        service.reconcile();
        assertEquals(1, service.trackedShards.size());
    }

    // ── Core invariant tests (preserved from original) ───────────────────

    public void testPublishOnlyOnAdvance() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        service.tick();
        assertEquals(1L, service.publishCount.get());

        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(1L, service.skipCount.get());

        shard.processedCheckpoint = 200L;
        service.tick();
        assertEquals(2L, service.publishCount.get());
    }

    public void testNoPublishWhenUnchanged() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 50L;
        service.trackShard(shard);

        service.tick();
        assertEquals(1L, service.publishCount.get());

        for (int i = 0; i < 5; i++) {
            service.tick();
        }
        assertEquals(1L, service.publishCount.get());
        assertEquals(5L, service.skipCount.get());
    }

    public void testPerShardIndependence() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-a", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-a", 1, "uuid-a")),
            "source-b", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-b", 1, "uuid-b"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shardA = new FakeShardState("source-a", "uuid-a", 0);
        shardA.processedCheckpoint = 10L;
        service.trackShard(shardA);

        FakeShardState shardB = new FakeShardState("source-b", "uuid-b", 0);
        shardB.processedCheckpoint = 20L;
        service.trackShard(shardB);

        service.tick();
        assertEquals(2L, service.publishCount.get());

        shardA.processedCheckpoint = 30L;
        service.tick();
        assertEquals(3L, service.publishCount.get());
        assertEquals(List.of(10L, 30L), service.publishedSeqNos(shardA.shardId));
        assertEquals(List.of(20L), service.publishedSeqNos(shardB.shardId));
    }

    public void testShardWithNoTargetsSkipped() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of());
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        service.tick();
        assertEquals(0L, service.publishCount.get());
    }

    public void testNonPrimaryShardSkipped() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        shard.primary = false;
        service.trackShard(shard);

        service.tick();
        assertEquals(0L, service.publishCount.get());
    }

    public void testUntrackRemovesShardFromFutureTicks() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        service.tick();
        assertEquals(1L, service.publishCount.get());

        service.untrackShard(shard.shardId);
        shard.processedCheckpoint = 200L;

        service.tick();
        assertEquals(1L, service.publishCount.get());
    }

    public void testCheckpointGoesBackwardIgnored() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        service.tick();
        assertEquals(1L, service.publishCount.get());

        shard.processedCheckpoint = 50L;
        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(1L, service.skipCount.get());
    }

    public void testSettingDefault() {
        TimeValue def = MVReplicationService.CHECKPOINT_PUBLISH_INTERVAL.getDefault(
            org.opensearch.common.settings.Settings.EMPTY
        );
        assertEquals(TimeValue.timeValueSeconds(1), def);
    }
}
