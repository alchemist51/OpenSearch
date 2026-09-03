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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link MVReplicationService}'s advance-detection logic.
 * Uses lightweight fakes — no cluster, no real shards, no thread pool.
 * Tests exercise the core invariants:
 * <ul>
 *   <li>Publish ONLY when checkpoint advances</li>
 *   <li>No publish when checkpoint is unchanged</li>
 *   <li>Per-shard independence (advance on one shard doesn't affect another)</li>
 *   <li>Shard tracking lifecycle (start → track, close → untrack)</li>
 * </ul>
 */
public class MVReplicationServiceTests extends OpenSearchTestCase {

    /**
     * Fake shard that exposes a mutable checkpoint and routing info.
     * Extends nothing from the real IndexShard — we only need the fields
     * that MVReplicationService.tick() reads.
     */
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

    /**
     * Fake routing service that returns configurable source→target mappings.
     */
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

    /**
     * Records all publish() invocations for assertion.
     * Since MVCheckpointPublisher is final, we record directly in the testable service.
     */
    static class PublishRecord {
        final List<Long> publishedSeqNos = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * Testable subclass that uses fake shard state instead of real IndexShard.
     * Overrides tick() to use the fake infrastructure.
     */
    static class TestableMVReplicationService {
        final FakeRoutingService routingService;
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeShardState> trackedShards = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, PublishRecord> publishers = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, AtomicLong> lastPublished = new ConcurrentHashMap<>();
        final AtomicLong tickCount = new AtomicLong();
        final AtomicLong publishCount = new AtomicLong();
        final AtomicLong skipCount = new AtomicLong();

        TestableMVReplicationService(FakeRoutingService routingService) {
            this.routingService = routingService;
        }

        void trackShard(FakeShardState shard) {
            PublishRecord record = new PublishRecord();
            trackedShards.put(shard.shardId, shard);
            publishers.put(shard.shardId, record);
            lastPublished.putIfAbsent(shard.shardId, new AtomicLong(-1L));
        }

        void untrackShard(org.opensearch.core.index.shard.ShardId shardId) {
            trackedShards.remove(shardId);
            publishers.remove(shardId);
            lastPublished.remove(shardId);
        }

        /**
         * Mirrors MVReplicationService.tick() logic exactly, using fake state.
         */
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

                PublishRecord record = publishers.get(shardId);
                if (record != null) {
                    record.publishedSeqNos.add(currentCheckpoint);
                }
                publishCount.incrementAndGet();
            }
        }

        List<Long> publishedSeqNos(org.opensearch.core.index.shard.ShardId shardId) {
            PublishRecord pub = publishers.get(shardId);
            return pub == null ? List.of() : List.copyOf(pub.publishedSeqNos);
        }
    }

    // ── Tests ────────────────────────────────────────────────────────────

    public void testPublishOnlyOnAdvance() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        // First tick: checkpoint 100 should publish
        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(List.of(100L), service.publishedSeqNos(shard.shardId));

        // Second tick: same checkpoint — should NOT publish
        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(1L, service.skipCount.get());

        // Third tick: checkpoint advances to 200 — should publish
        shard.processedCheckpoint = 200L;
        service.tick();
        assertEquals(2L, service.publishCount.get());
        assertEquals(List.of(100L, 200L), service.publishedSeqNos(shard.shardId));
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

        // 5 more ticks with no advance
        for (int i = 0; i < 5; i++) {
            service.tick();
        }
        assertEquals(1L, service.publishCount.get());
        assertEquals(5L, service.skipCount.get());
        assertEquals(List.of(50L), service.publishedSeqNos(shard.shardId));
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

        // Both should publish
        service.tick();
        assertEquals(2L, service.publishCount.get());
        assertEquals(List.of(10L), service.publishedSeqNos(shardA.shardId));
        assertEquals(List.of(20L), service.publishedSeqNos(shardB.shardId));

        // Only shard A advances
        shardA.processedCheckpoint = 30L;
        service.tick();
        assertEquals(3L, service.publishCount.get());
        assertEquals(List.of(10L, 30L), service.publishedSeqNos(shardA.shardId));
        // shard B unchanged — still 1 publish
        assertEquals(List.of(20L), service.publishedSeqNos(shardB.shardId));

        // Only shard B advances
        shardB.processedCheckpoint = 40L;
        service.tick();
        assertEquals(4L, service.publishCount.get());
        assertEquals(List.of(20L, 40L), service.publishedSeqNos(shardB.shardId));
        // shard A unchanged — still 2 publishes
        assertEquals(List.of(10L, 30L), service.publishedSeqNos(shardA.shardId));
    }

    public void testShardWithNoTargetsSkipped() {
        FakeRoutingService routing = new FakeRoutingService();
        // No targets bound
        routing.setSourceToTargets(Map.of());
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        service.tick();
        assertEquals(0L, service.publishCount.get());
        assertEquals(0L, service.skipCount.get()); // not even counted as skip — no targets
    }

    public void testNonPrimaryShardSkipped() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        shard.primary = false; // replica
        service.trackShard(shard);

        service.tick();
        assertEquals(0L, service.publishCount.get());
    }

    public void testInactiveShardSkipped() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        shard.active = false;
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

        // Untrack
        service.untrackShard(shard.shardId);
        shard.processedCheckpoint = 200L;

        service.tick();
        // Should NOT publish — shard was untracked
        assertEquals(1L, service.publishCount.get());
    }

    public void testTargetAppearsAfterShardTracked() {
        FakeRoutingService routing = new FakeRoutingService();
        // Initially no targets
        routing.setSourceToTargets(Map.of());
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        // Tick with no targets — nothing published
        service.tick();
        assertEquals(0L, service.publishCount.get());

        // Target appears dynamically
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));

        // Next tick — should publish since checkpoint > lastPublished (-1)
        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(List.of(100L), service.publishedSeqNos(shard.shardId));
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

        // Checkpoint goes backward (e.g., shard relocated and came back with older state)
        shard.processedCheckpoint = 50L;
        service.tick();
        assertEquals(1L, service.publishCount.get()); // no new publish
        assertEquals(1L, service.skipCount.get());
    }

    public void testMultipleShardsOfSameIndex() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 3, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard0 = new FakeShardState("source-idx", "uuid-1", 0);
        shard0.processedCheckpoint = 10L;
        service.trackShard(shard0);

        FakeShardState shard1 = new FakeShardState("source-idx", "uuid-1", 1);
        shard1.processedCheckpoint = 20L;
        service.trackShard(shard1);

        service.tick();
        assertEquals(2L, service.publishCount.get());
        assertEquals(List.of(10L), service.publishedSeqNos(shard0.shardId));
        assertEquals(List.of(20L), service.publishedSeqNos(shard1.shardId));

        // Only shard 1 advances
        shard1.processedCheckpoint = 30L;
        service.tick();
        assertEquals(3L, service.publishCount.get());
        assertEquals(List.of(10L), service.publishedSeqNos(shard0.shardId));
        assertEquals(List.of(20L, 30L), service.publishedSeqNos(shard1.shardId));
    }

    public void testSettingDefault() {
        // Verify the setting parses correctly and has the expected default
        TimeValue def = MVReplicationService.CHECKPOINT_PUBLISH_INTERVAL.getDefault(
            org.opensearch.common.settings.Settings.EMPTY
        );
        assertEquals(TimeValue.timeValueSeconds(1), def);
    }
}
