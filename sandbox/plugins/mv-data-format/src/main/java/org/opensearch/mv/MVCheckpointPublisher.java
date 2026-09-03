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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Source-side checkpoint publisher: after a source shard's parquet generation
 * is uploaded, pushes an advert to each bound target shard's node. This
 * replaces the target's remote-store listing+metadata init on every poll round
 * with a source-pushed notification.
 *
 * <p>Throttle: only publishes when maxSeqNo advances beyond the last published
 * value (coalesce). Fire-and-forget: publish failures are logged but do not
 * block the source refresh. The target's poller fallback to pull mode handles
 * missed pushes.
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

    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong publishFailures = new AtomicLong();

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
     * @param maxSeqNo      the maximum sequence number in the published generation
     * @param primaryTerm   the source shard's primary term
     * @param infosVersion  the segment infos version of the published generation
     * @param parquetFiles  the parquet file names in this generation
     * @param fileSizes     per-file byte sizes (parallel to parquetFiles, -1 if unknown)
     */
    public void publish(long maxSeqNo, long primaryTerm, long infosVersion, List<String> parquetFiles, List<Long> fileSizes) {
        // Coalesce: skip if maxSeqNo hasn't advanced
        long last = lastPublishedSeqNo.get();
        if (maxSeqNo <= last) {
            return;
        }
        // CAS to prevent concurrent publishes for the same seqNo
        if (lastPublishedSeqNo.compareAndSet(last, maxSeqNo) == false) {
            return; // Another thread already publishing a newer checkpoint
        }

        List<NodeRoutingSnapshotService.BoundTarget> targets = routingService.sourceToTargets().get(sourceIndex);
        if (targets == null || targets.isEmpty()) {
            return; // No bound MV targets for this source
        }

        for (NodeRoutingSnapshotService.BoundTarget target : targets) {
            // Validate source UUID matches the binding (stale binding detection)
            if (sourceUuid != null && target.sourceUuid() != null && sourceUuid.equals(target.sourceUuid()) == false) {
                logger.warn(
                    "checkpoint_publish: skipping target [{}] — bound sourceUuid [{}] != live [{}]",
                    target.targetIndex(),
                    target.sourceUuid(),
                    sourceUuid
                );
                continue;
            }

            // Resolve target shard via modular mapping (same as ship path)
            int targetShardId = target.targetShards() > 0 ? sourceShard % target.targetShards() : 0;

            MVCheckpointPublishAction.Request request = new MVCheckpointPublishAction.Request(
                target.targetIndex(),
                targetShardId,
                sourceIndex,
                sourceUuid,
                sourceShard,
                maxSeqNo,
                primaryTerm,
                infosVersion,
                parquetFiles,
                fileSizes
            );

            publishCount.incrementAndGet();
            // Fire-and-forget: do not block the source refresh
            client.execute(MVCheckpointPublishAction.INSTANCE, request, ActionListener.wrap(response -> {
                if (response.accepted()) {
                    logger.debug(
                        "checkpoint_publish: target [{}][{}] accepted maxSeqNo={}",
                        target.targetIndex(),
                        targetShardId,
                        maxSeqNo
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

    public long publishCount() {
        return publishCount.get();
    }

    public long publishFailures() {
        return publishFailures.get();
    }
}
