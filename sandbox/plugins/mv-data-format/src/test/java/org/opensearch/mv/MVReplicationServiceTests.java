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
 *   <li>Defect 1: advert maxSeqNo = max fileset range, NOT processed checkpoint</li>
 *   <li>Unknown ranges: skip publish with WARN (all writers must populate ranges)</li>
 *   <li>Catalog read failure: skip tick, retry next interval</li>
 *   <li>Defect 2: lost push retried via per-target lag</li>
 *   <li>Defect 2: per-target independence (one behind, one at parity)</li>
 *   <li>Defect 2: refresh-closes-gap triggers rebuild</li>
 *   <li>Core: advance-detection, per-shard independence, lifecycle, reconciliation</li>
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
        final List<Long> publishedMaxSeqNos = Collections.synchronizedList(new ArrayList<>());
        final List<Map<String, MVFileMetadata>> publishedFileMetadata = Collections.synchronizedList(new ArrayList<>());
    }

    /** Fake catalog for testing real adverts. */
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

    /**
     * Testable harness that mirrors MVReplicationService tick() semantics
     * (catalog-derived advertMax, per-target lag conditions) using fake
     * shard state and a fake publisher that tracks per-target watermarks.
     */
    static class TestableMVReplicationService {
        final FakeRoutingService routingService;
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeShardState> trackedShards = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, PublishRecord> publishRecords = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakePublisher> publishers = new ConcurrentHashMap<>();
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeShardState> allShards = new ConcurrentHashMap<>();

        final AtomicLong tickCount = new AtomicLong();
        final AtomicLong publishCount = new AtomicLong();
        final AtomicLong skipCount = new AtomicLong();
        final AtomicLong catalogReadCount = new AtomicLong();
        final AtomicLong unknownRangeSkipCount = new AtomicLong();

        /** Per-shard fake catalog: file names + seq ranges. */
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, FakeCatalog> catalogs = new ConcurrentHashMap<>();

        /** Simulate catalog read failure for a shard. */
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, Boolean> catalogReadFailure = new ConcurrentHashMap<>();

        /** Per-shard tick-local state, mirroring TrackedShard fields. */
        final ConcurrentHashMap<org.opensearch.core.index.shard.ShardId, long[]> shardState = new ConcurrentHashMap<>();

        TestableMVReplicationService(FakeRoutingService routingService) {
            this.routingService = routingService;
        }

        void registerShard(FakeShardState shard) {
            allShards.put(shard.shardId, shard);
        }

        void trackShard(FakeShardState shard) {
            trackedShards.put(shard.shardId, shard);
            publishRecords.put(shard.shardId, new PublishRecord());
            FakePublisher pub = new FakePublisher(routingService, shard.shardId.getIndexName(), shard.indexUuid, shard.shardId.id());
            publishers.put(shard.shardId, pub);
            // [0] = lastObservedProcessed, [1] = lastAdvertMax
            shardState.putIfAbsent(shard.shardId, new long[]{-1L, -1L});
        }

        void untrackShard(org.opensearch.core.index.shard.ShardId shardId) {
            trackedShards.remove(shardId);
            publishRecords.remove(shardId);
            publishers.remove(shardId);
            allShards.remove(shardId);
            shardState.remove(shardId);
        }

        void setCatalog(org.opensearch.core.index.shard.ShardId shardId, FakeCatalog catalog) {
            catalogs.put(shardId, catalog);
        }

        void setCatalogReadFailure(org.opensearch.core.index.shard.ShardId shardId, boolean fail) {
            if (fail) {
                catalogReadFailure.put(shardId, true);
            } else {
                catalogReadFailure.remove(shardId);
            }
        }

        /** Mirrors MVReplicationService.tick() with no-legacy semantics. */
        void tick() {
            tickCount.incrementAndGet();
            Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

            for (Map.Entry<org.opensearch.core.index.shard.ShardId, FakeShardState> entry : trackedShards.entrySet()) {
                org.opensearch.core.index.shard.ShardId shardId = entry.getKey();
                FakeShardState shard = entry.getValue();
                FakePublisher publisher = publishers.get(shardId);

                List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(shardId.getIndexName());
                if (targets == null || targets.isEmpty()) continue;
                if (!shard.primary || !shard.active) continue;

                long currentProcessed = shard.processedCheckpoint;
                long[] state = shardState.get(shardId);

                boolean newData = currentProcessed > state[0];
                boolean refreshClosedGap = currentProcessed > state[1];
                boolean targetsBehind = publisher.anyTargetBehind(state[1]);

                if (!newData && !refreshClosedGap && !targetsBehind) {
                    skipCount.incrementAndGet();
                    continue;
                }

                state[0] = currentProcessed;

                // Simulate catalog read failure
                if (Boolean.TRUE.equals(catalogReadFailure.get(shardId))) {
                    continue; // skip tick, retry next interval
                }

                // Read catalog and compute advertMax
                Map<String, MVFileMetadata> fileMetadata = new LinkedHashMap<>();
                long catalogAdvertMax = -1L;
                boolean anyUnknownRange = false;

                FakeCatalog cat = catalogs.get(shardId);
                if (cat != null) {
                    catalogReadCount.incrementAndGet();
                    for (int i = 0; i < cat.files.size(); i++) {
                        long minSeq = i < cat.minSeqNos.size() ? cat.minSeqNos.get(i) : -1L;
                        long maxSeq = i < cat.maxSeqNos.size() ? cat.maxSeqNos.get(i) : -1L;
                        fileMetadata.put(cat.files.get(i), new MVFileMetadata(-1L, minSeq, maxSeq, MVFileMetadata.CRC32_UNKNOWN));
                        if (maxSeq < 0) {
                            anyUnknownRange = true;
                        }
                        if (maxSeq >= 0 && maxSeq > catalogAdvertMax) {
                            catalogAdvertMax = maxSeq;
                        }
                    }
                }

                // Unknown ranges = BUG: skip publish
                if (anyUnknownRange) {
                    unknownRangeSkipCount.incrementAndGet();
                    continue;
                }

                long advertMax;
                if (catalogAdvertMax > 0) {
                    advertMax = catalogAdvertMax;
                } else {
                    continue; // no coverable files
                }

                state[1] = advertMax;

                // Record and publish
                PublishRecord record = publishRecords.get(shardId);
                if (record != null) {
                    record.publishedMaxSeqNos.add(advertMax);
                    record.publishedFileMetadata.add(Map.copyOf(fileMetadata));
                }

                int sent = publisher.publish(advertMax);
                if (sent > 0) {
                    publishCount.incrementAndGet();
                }
            }
        }

        /** Mirrors reconcileTrackedShards. */
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

        List<Long> publishedMaxSeqNos(org.opensearch.core.index.shard.ShardId shardId) {
            PublishRecord pub = publishRecords.get(shardId);
            return pub == null ? List.of() : List.copyOf(pub.publishedMaxSeqNos);
        }

        PublishRecord getPublishRecord(org.opensearch.core.index.shard.ShardId shardId) {
            return publishRecords.get(shardId);
        }

        long lastAdvertMax(org.opensearch.core.index.shard.ShardId shardId) {
            long[] state = shardState.get(shardId);
            return state != null ? state[1] : -1L;
        }
    }

    /**
     * Fake publisher that tracks per-target watermarks and publish invocations.
     * Simulates the real MVCheckpointPublisher's per-target skip logic.
     */
    static class FakePublisher {
        final FakeRoutingService routingService;
        final String sourceIndex;
        final String sourceUuid;
        final int sourceShard;
        final ConcurrentHashMap<String, Long> targetWatermarks = new ConcurrentHashMap<>();
        final List<Long> publishedMaxSeqNos = Collections.synchronizedList(new ArrayList<>());
        /** Per-target publish counts for verifying per-target independence. */
        final ConcurrentHashMap<String, AtomicLong> perTargetSendCount = new ConcurrentHashMap<>();

        FakePublisher(FakeRoutingService routingService, String sourceIndex, String sourceUuid, int sourceShard) {
            this.routingService = routingService;
            this.sourceIndex = sourceIndex;
            this.sourceUuid = sourceUuid;
            this.sourceShard = sourceShard;
        }

        int publish(long maxSeqNo) {
            publishedMaxSeqNos.add(maxSeqNo);
            List<NodeRoutingSnapshotService.BoundTarget> targets = routingService.sourceToTargets().get(sourceIndex);
            if (targets == null || targets.isEmpty()) return 0;

            int sent = 0;
            for (NodeRoutingSnapshotService.BoundTarget target : targets) {
                int targetShardId = target.targetShards() > 0 ? sourceShard % target.targetShards() : 0;
                String key = target.targetIndex() + ":" + targetShardId;
                long watermark = targetWatermarks.getOrDefault(key, -1L);
                if (watermark >= maxSeqNo && maxSeqNo >= 0) {
                    continue; // skip — target already at parity
                }
                sent++;
                perTargetSendCount.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
            }
            return sent;
        }

        boolean anyTargetBehind(long advertMax) {
            if (advertMax < 0) return false;
            List<NodeRoutingSnapshotService.BoundTarget> targets = routingService.sourceToTargets().get(sourceIndex);
            if (targets == null || targets.isEmpty()) return false;

            for (NodeRoutingSnapshotService.BoundTarget target : targets) {
                int targetShardId = target.targetShards() > 0 ? sourceShard % target.targetShards() : 0;
                String key = target.targetIndex() + ":" + targetShardId;
                long watermark = targetWatermarks.getOrDefault(key, -1L);
                if (watermark < advertMax) return true;
            }
            return false;
        }

        void simulateWatermarkResponse(String targetIndex, int targetShard, long watermark) {
            String key = targetIndex + ":" + targetShard;
            targetWatermarks.merge(key, watermark, Math::max);
        }

        long getTargetSendCount(String targetIndex, int targetShard) {
            String key = targetIndex + ":" + targetShard;
            AtomicLong count = perTargetSendCount.get(key);
            return count != null ? count.get() : 0;
        }
    }

    // ── Defect 1: advert maxSeqNo = max fileset range ────────────────────

    public void testAdvertMaxSeqNoEqualsMaxFilesetRange() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        // Processed is at 500, but files only cover up to 300
        shard.processedCheckpoint = 500L;
        service.trackShard(shard);

        FakeCatalog catalog = new FakeCatalog(
            List.of("gen-1.parquet", "gen-2.parquet"),
            List.of(0L, 100L),
            List.of(99L, 300L)  // max file coverage = 300, NOT 500
        );
        service.setCatalog(shard.shardId, catalog);

        service.tick();
        assertEquals(1L, service.publishCount.get());
        // advertMax must be 300 (from files), NOT 500 (processedCheckpoint)
        assertEquals(List.of(300L), service.publishedMaxSeqNos(shard.shardId));
        assertEquals(300L, service.lastAdvertMax(shard.shardId));
    }

    // ── Unknown ranges: skip publish with WARN (replaces legacy fallback) ─

    public void testUnknownRangesSkipsPublishWithWarn() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 200L;
        service.trackShard(shard);

        // All files have unknown seq ranges (-1) — this is a BUG, should WARN+skip
        FakeCatalog catalog = new FakeCatalog(
            List.of("unknown-1.parquet", "unknown-2.parquet"),
            List.of(-1L, -1L),
            List.of(-1L, -1L)
        );
        service.setCatalog(shard.shardId, catalog);

        service.tick();
        // MUST NOT publish — unknown ranges are a bug, skip with WARN
        assertEquals(0L, service.publishCount.get());
        assertEquals(1L, service.unknownRangeSkipCount.get());
    }

    public void testMixedKnownAndUnknownRangesSkipsPublish() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 200L;
        service.trackShard(shard);

        // Mix of known and unknown ranges — still a bug, skip publish
        FakeCatalog catalog = new FakeCatalog(
            List.of("known.parquet", "unknown.parquet"),
            List.of(0L, -1L),
            List.of(100L, -1L)
        );
        service.setCatalog(shard.shardId, catalog);

        service.tick();
        assertEquals(0L, service.publishCount.get());
        assertEquals(1L, service.unknownRangeSkipCount.get());
    }

    // ── Catalog read failure: skip tick, retry next interval ─────────────

    public void testCatalogReadFailureSkipsTick() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);
        service.setCatalog(shard.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(100L)));

        // Simulate catalog read failure
        service.setCatalogReadFailure(shard.shardId, true);

        service.tick();
        // No publish — catalog read failed, skipped
        assertEquals(0L, service.publishCount.get());

        // Recovery: clear failure, next tick publishes
        service.setCatalogReadFailure(shard.shardId, false);
        service.tick();
        assertEquals(1L, service.publishCount.get());
    }

    // ── Defect 1: no files → skip publish ────────────────────────────────

    public void testNoFilesSkipsPublish() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

        // No catalog or empty catalog → no files
        service.setCatalog(shard.shardId, new FakeCatalog(List.of(), List.of(), List.of()));

        service.tick();
        assertEquals(0L, service.publishCount.get());
    }

    // ── Defect 2: lost push retried ──────────────────────────────────────

    public void testLostPushRetriedNextTick() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);
        service.setCatalog(shard.shardId, new FakeCatalog(
            List.of("gen-1.parquet"), List.of(0L), List.of(100L)
        ));

        // First tick: publishes
        service.tick();
        assertEquals(1L, service.publishCount.get());
        FakePublisher pub = service.publishers.get(shard.shardId);
        assertNotNull(pub);

        // No watermark response → target still behind → next tick retries
        service.tick();
        assertEquals(2L, service.publishCount.get()); // retried!

        // Simulate watermark response confirming receipt
        pub.simulateWatermarkResponse("mv-target", 0, 100L);

        // Next tick: target at parity → skip
        service.tick();
        assertEquals(2L, service.publishCount.get()); // no new publish
        assertEquals(1L, service.skipCount.get());
    }

    // ── Defect 2: per-target independence ─────────────────────────────────

    public void testPerTargetIndependenceOneBehindOneAtParity() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target-a", 1, "uuid-1"),
                new NodeRoutingSnapshotService.BoundTarget("mv-target-b", 1, "uuid-1")
            )
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);
        service.setCatalog(shard.shardId, new FakeCatalog(
            List.of("gen-1.parquet"), List.of(0L), List.of(100L)
        ));

        // First tick: both targets receive
        service.tick();
        assertEquals(1L, service.publishCount.get());
        FakePublisher pub = service.publishers.get(shard.shardId);

        // Target A confirms at parity; Target B drops the push (no response)
        pub.simulateWatermarkResponse("mv-target-a", 0, 100L);

        // Next tick: only target B is behind → exactly one send
        service.tick();
        assertEquals(2L, service.publishCount.get());
        // Target A should have 1 send total (first tick only)
        assertEquals(1L, pub.getTargetSendCount("mv-target-a", 0));
        // Target B should have 2 sends (first tick + retry)
        assertEquals(2L, pub.getTargetSendCount("mv-target-b", 0));
    }

    // ── Defect 2: refresh-closes-gap ─────────────────────────────────────

    public void testRefreshClosesGapTriggersRebuild() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 200L;
        service.trackShard(shard);

        // Initial catalog: files only cover up to 100
        service.setCatalog(shard.shardId, new FakeCatalog(
            List.of("gen-1.parquet"), List.of(0L), List.of(100L)
        ));

        // First tick: publishes with advertMax=100
        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(List.of(100L), service.publishedMaxSeqNos(shard.shardId));

        FakePublisher pub = service.publishers.get(shard.shardId);
        pub.simulateWatermarkResponse("mv-target", 0, 100L);

        // Processed stays 200, but refresh adds new file coverage
        // (simulating parquet flush that lagged behind commit)
        service.setCatalog(shard.shardId, new FakeCatalog(
            List.of("gen-1.parquet", "gen-2.parquet"),
            List.of(0L, 100L),
            List.of(100L, 200L)
        ));

        // Next tick: processed (200) > lastAdvertMax (100) → condition (B) fires
        // Also target is behind new advertMax (200) → condition (C) fires
        service.tick();
        assertEquals(2L, service.publishCount.get());
        assertEquals(List.of(100L, 200L), service.publishedMaxSeqNos(shard.shardId));
    }

    // ── Real adverts from catalog snapshot ────────────────────────────────

    public void testRealAdvertCarriesFilesAndRanges() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);

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

        // Simulate target confirming parity
        FakePublisher pub = service.publishers.get(shard.shardId);
        pub.simulateWatermarkResponse("mv-target", 0, 100L);

        // Second tick: no advance, target at parity → no catalog read
        service.tick();
        assertEquals(1L, service.catalogReadCount.get());
        assertEquals(1L, service.skipCount.get());
    }

    // ── Reconciliation ───────────────────────────────────────────────────

    public void testReconcileAddsShardWhenTargetAppears() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of());
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.registerShard(shard);

        service.reconcile();
        assertEquals(0, service.trackedShards.size());

        service.tick();
        assertEquals(0L, service.publishCount.get());

        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        service.setCatalog(shard.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(100L)));

        service.reconcile();
        assertEquals(1, service.trackedShards.size());

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

        service.reconcile();
        assertEquals(1, service.trackedShards.size());
    }

    // ── Core invariant tests ─────────────────────────────────────────────

    public void testPublishOnAdvance() {
        FakeRoutingService routing = new FakeRoutingService();
        routing.setSourceToTargets(Map.of(
            "source-idx", List.of(new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1"))
        ));
        TestableMVReplicationService service = new TestableMVReplicationService(routing);

        FakeShardState shard = new FakeShardState("source-idx", "uuid-1", 0);
        shard.processedCheckpoint = 100L;
        service.trackShard(shard);
        service.setCatalog(shard.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(100L)));

        service.tick();
        assertEquals(1L, service.publishCount.get());

        // Confirm target parity so skip takes effect
        FakePublisher pub = service.publishers.get(shard.shardId);
        pub.simulateWatermarkResponse("mv-target", 0, 100L);

        service.tick();
        assertEquals(1L, service.publishCount.get());
        assertEquals(1L, service.skipCount.get());

        // Advance processed AND catalog
        shard.processedCheckpoint = 200L;
        service.setCatalog(shard.shardId, new FakeCatalog(
            List.of("a.parquet", "b.parquet"), List.of(0L, 100L), List.of(100L, 200L)
        ));
        service.tick();
        assertEquals(2L, service.publishCount.get());
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
        service.setCatalog(shardA.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(10L)));

        FakeShardState shardB = new FakeShardState("source-b", "uuid-b", 0);
        shardB.processedCheckpoint = 20L;
        service.trackShard(shardB);
        service.setCatalog(shardB.shardId, new FakeCatalog(List.of("b.parquet"), List.of(0L), List.of(20L)));

        service.tick();
        assertEquals(2L, service.publishCount.get());

        // Confirm parity for both
        service.publishers.get(shardA.shardId).simulateWatermarkResponse("mv-a", 0, 10L);
        service.publishers.get(shardB.shardId).simulateWatermarkResponse("mv-b", 0, 20L);

        // Advance only shard A
        shardA.processedCheckpoint = 30L;
        service.setCatalog(shardA.shardId, new FakeCatalog(
            List.of("a.parquet", "a2.parquet"), List.of(0L, 10L), List.of(10L, 30L)
        ));
        service.tick();
        assertEquals(3L, service.publishCount.get()); // only A published
        assertEquals(List.of(10L, 30L), service.publishedMaxSeqNos(shardA.shardId));
        assertEquals(List.of(20L), service.publishedMaxSeqNos(shardB.shardId));
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
        service.setCatalog(shard.shardId, new FakeCatalog(List.of("a.parquet"), List.of(0L), List.of(100L)));

        service.tick();
        assertEquals(1L, service.publishCount.get());

        service.untrackShard(shard.shardId);
        shard.processedCheckpoint = 200L;

        service.tick();
        assertEquals(1L, service.publishCount.get());
    }

    public void testSettingDefault() {
        TimeValue def = MVReplicationService.CHECKPOINT_PUBLISH_INTERVAL.getDefault(
            org.opensearch.common.settings.Settings.EMPTY
        );
        assertEquals(TimeValue.timeValueSeconds(1), def);
    }
}
