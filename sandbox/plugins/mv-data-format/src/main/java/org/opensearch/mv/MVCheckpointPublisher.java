/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;
import org.opensearch.transport.client.Client;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Source-side checkpoint publisher: after a source shard's parquet generation
 * is uploaded, pushes an {@link MVReplicationCheckpoint} to each bound target
 * shard's node. This replaces the target's remote-store listing+metadata init
 * on every poll round with a source-pushed notification.
 *
 * <p>Throttle: only publishes when maxSeqNo advances beyond the last published
 * value (coalesce). Fire-and-forget: publish failures are logged but do not
 * block the source refresh. The target's poller fallback to pull mode handles
 * missed pushes.
 *
 * <p>Maintains per-target-shard watermark state learned from publish responses.
 * When building checkpoints, files whose seq range falls entirely at or below
 * the target's watermark are excluded — the target already has that data.
 * Legacy files (unknown seq range) are always included (fail-open).
 *
 * <p>SAFETY: never calls clusterService.state() — all routing data comes from
 * the lock-free {@link NodeRoutingSnapshotService} maintained by a
 * ClusterStateListener. Safe to call from engine callbacks including the
 * cluster-applier thread.
 */
public final class MVCheckpointPublisher {

    private static final Logger logger = LogManager.getLogger(MVCheckpointPublisher.class);

    private final Client client;
    private final String sourceIndex;
    private final String sourceUuid;
    private final int sourceShard;
    private final NodeRoutingSnapshotService routingService;

    /** Last maxSeqNo that was successfully published — coalesce throttle. */
    private final AtomicLong lastPublishedSeqNo = new AtomicLong(-1L);

    /**
     * Per-target-shard last-known watermark, updated from publish responses.
     * Key: "targetIndex:targetShard". Value: highest watermark seen from that target.
     * Used for source-side file filtering — files fully below the watermark are excluded.
     */
    private final ConcurrentHashMap<String, AtomicLong> targetWatermarks = new ConcurrentHashMap<>();

    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong publishFailures = new AtomicLong();
    private final AtomicLong filesFilteredCount = new AtomicLong();

    public MVCheckpointPublisher(
        Client client,
        String sourceIndex,
        String sourceUuid,
        int sourceShard,
        NodeRoutingSnapshotService routingService
    ) {
        this.client = client;
        this.sourceIndex = sourceIndex;
        this.sourceUuid = sourceUuid;
        this.sourceShard = sourceShard;
        this.routingService = routingService;
    }

    /**
     * Publishes a checkpoint to all bound target shards. Called after the
     * source shard's refresh completes and parquet files are uploaded.
     *
     * @param checkpoint  the fully-built MVReplicationCheckpoint from the catalog snapshot
     */
    public void publish(MVReplicationCheckpoint checkpoint) {
        long maxSeqNo = checkpoint.maxSeqNo();

        // Coalesce: skip if maxSeqNo hasn't advanced
        long last = lastPublishedSeqNo.get();
        if (maxSeqNo <= last) {
            return;
        }
        // CAS to prevent concurrent publishes for the same seqNo
        if (lastPublishedSeqNo.compareAndSet(last, maxSeqNo) == false) {
            return;
        }

        List<NodeRoutingSnapshotService.BoundTarget> targets = routingService.sourceToTargets().get(sourceIndex);
        if (targets == null || targets.isEmpty()) {
            return;
        }

        for (NodeRoutingSnapshotService.BoundTarget target : targets) {
            if (sourceUuid != null && target.sourceUuid() != null && sourceUuid.equals(target.sourceUuid()) == false) {
                logger.warn(
                    "checkpoint_publish: skipping target [{}] — bound sourceUuid [{}] != live [{}]",
                    target.targetIndex(),
                    target.sourceUuid(),
                    sourceUuid
                );
                continue;
            }

            int targetShardId = target.targetShards() > 0 ? sourceShard % target.targetShards() : 0;

            // Source-side file filtering using checkpoint metadata map
            String watermarkKey = target.targetIndex() + ":" + targetShardId;
            long watermark = getLastKnownWatermark(watermarkKey);

            MVReplicationCheckpoint scopedCheckpoint;
            if (watermark == -1L || checkpoint.fileMetadata().isEmpty()) {
                scopedCheckpoint = checkpoint;
            } else {
                Map<String, MVFileMetadata> scopedFiles = new LinkedHashMap<>();
                int totalFiles = checkpoint.fileMetadata().size();
                for (Map.Entry<String, MVFileMetadata> entry : checkpoint.fileMetadata().entrySet()) {
                    MVFileMetadata meta = entry.getValue();
                    if (includeFile(meta.minSeqNo(), meta.maxSeqNo(), watermark, maxSeqNo)) {
                        scopedFiles.put(entry.getKey(), meta);
                    }
                }
                int filtered = totalFiles - scopedFiles.size();
                if (filtered > 0) {
                    filesFilteredCount.addAndGet(filtered);
                    logger.debug(
                        "ADVERT_SCOPED target=[{}][{}] files_total={} files_sent={} watermark_used={}",
                        target.targetIndex(),
                        targetShardId,
                        totalFiles,
                        scopedFiles.size(),
                        watermark
                    );
                }
                scopedCheckpoint = new MVReplicationCheckpoint(
                    checkpoint.sourceIndex(),
                    checkpoint.sourceShard(),
                    checkpoint.primaryTerm(),
                    checkpoint.maxSeqNo(),
                    checkpoint.infosVersion(),
                    scopedFiles,
                    checkpoint.createdTimeStampMillis()
                );
            }

            MVCheckpointPublishAction.Request request = new MVCheckpointPublishAction.Request(
                target.targetIndex(),
                targetShardId,
                sourceUuid,
                scopedCheckpoint
            );

            publishCount.incrementAndGet();
            client.execute(MVCheckpointPublishAction.INSTANCE, request, ActionListener.wrap(response -> {
                if (response.accepted()) {
                    updateTargetWatermark(watermarkKey, response.targetWatermark());
                    logger.debug(
                        "checkpoint_publish: target [{}][{}] accepted maxSeqNo={} targetWatermark={}",
                        target.targetIndex(),
                        targetShardId,
                        maxSeqNo,
                        response.targetWatermark()
                    );
                } else {
                    logger.debug(
                        "checkpoint_publish: target [{}][{}] rejected maxSeqNo={} (not ready)",
                        target.targetIndex(),
                        targetShardId,
                        maxSeqNo
                    );
                }
            }, failure -> {
                publishFailures.incrementAndGet();
                logger.debug(
                    "checkpoint_publish: failed to push to target [{}][{}] maxSeqNo={}: {}",
                    target.targetIndex(),
                    targetShardId,
                    maxSeqNo,
                    failure.getMessage()
                );
            }));
        }
    }

    /**
     * Determines whether a file should be included in the advert for a target.
     * A file is included if:
     * <ul>
     *   <li>Its maxSeqNo is unknown (-1) — legacy/fail-open, always include</li>
     *   <li>Its [minSeqNo, maxSeqNo] range intersects (watermark, sourceMaxSeqNo]</li>
     * </ul>
     * A file is excluded only when its entire seq range is at or below the watermark.
     */
    static boolean includeFile(long fileMinSeqNo, long fileMaxSeqNo, long targetWatermark, long sourceMaxSeqNo) {
        if (fileMaxSeqNo == -1L) {
            return true;
        }
        if (fileMaxSeqNo <= targetWatermark) {
            return false;
        }
        return true;
    }

    void updateTargetWatermark(String key, long reportedWatermark) {
        if (reportedWatermark < 0) return;
        targetWatermarks.compute(key, (k, existing) -> {
            if (existing == null) {
                return new AtomicLong(reportedWatermark);
            }
            existing.accumulateAndGet(reportedWatermark, Math::max);
            return existing;
        });
    }

    long getLastKnownWatermark(String key) {
        AtomicLong wm = targetWatermarks.get(key);
        return wm != null ? wm.get() : -1L;
    }

    public long publishCount() {
        return publishCount.get();
    }

    public long publishFailures() {
        return publishFailures.get();
    }

    public long filesFilteredCount() {
        return filesFilteredCount.get();
    }

    ConcurrentHashMap<String, AtomicLong> targetWatermarks() {
        return targetWatermarks;
    }
}
