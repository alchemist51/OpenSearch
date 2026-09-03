/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.mv.MVReplicationCheckpoint;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-global checkpoint mailbox: receives pushed {@link MVReplicationCheckpoint}
 * adverts from source shards and delivers them to the target shard's
 * {@link MVDerivedSourceReader}.
 *
 * <p>Each slot is keyed by {@code targetIndex:targetShard:sourceIndex:sourceShard}
 * and holds exactly one (latest) checkpoint. Pushes are coalesced — a newer
 * checkpoint (per {@link MVReplicationCheckpoint#isAheadOf}) overwrites the
 * previous one. This is the MAILBOX ORDERING FIX: coalesce/consume decisions
 * use term-first ordering, so an old-term advert is always superseded by a
 * new-term advert (failover correctness).
 *
 * <p>The poller's {@code fetchSnapshot} call atomically consumes (reads and
 * clears) the slot.
 *
 * <p>Thread safety: all operations are lock-free via {@link ConcurrentHashMap}
 * and volatile reads. Multiple source shards may push concurrently; the poller
 * reads on the GENERIC thread pool.
 */
public final class MVCheckpointMailbox {

    private static final Logger logger = LogManager.getLogger(MVCheckpointMailbox.class);

    /** Singleton instance — one per node (created at plugin component creation). */
    private static volatile MVCheckpointMailbox INSTANCE;

    private final ConcurrentMap<String, MVReplicationCheckpoint> slots = new ConcurrentHashMap<>();
    private final AtomicLong pushCount = new AtomicLong();
    private final AtomicLong consumeCount = new AtomicLong();
    /**
     * Per-slot last consumed maxSeqNo — the target's effective watermark as
     * seen by the mailbox. Updated on every {@link #consume} call. Key format
     * matches {@link #slotKey}. Returned to the source via the publish response
     * to enable source-side file scoping.
     */
    private final ConcurrentMap<String, AtomicLong> lastConsumedWatermarks = new ConcurrentHashMap<>();

    public MVCheckpointMailbox() {}

    public static MVCheckpointMailbox instance() {
        return INSTANCE;
    }

    public static void setInstance(MVCheckpointMailbox instance) {
        INSTANCE = instance;
    }

    /**
     * Delivers a pushed checkpoint from a source shard. Overwrites any existing
     * checkpoint for the same slot if the new one isAheadOf the existing one
     * (term-first ordering — failover correctness).
     */
    public void deliver(String targetIndex, int targetShard, MVReplicationCheckpoint checkpoint) {
        String key = slotKey(targetIndex, targetShard, checkpoint.sourceIndex(), checkpoint.sourceShard());
        slots.merge(key, checkpoint, (existing, incoming) -> {
            // Term-first ordering: new-term always supersedes old-term
            if (incoming.isAheadOf(existing)) {
                return incoming;
            }
            return existing;
        });
        pushCount.incrementAndGet();
        logger.debug(
            "PUSH_CHECKPOINT_RECEIVED target=[{}][{}] source=[{}][{}] term={} maxSeqNo={} infosVersion={} files={}",
            targetIndex,
            targetShard,
            checkpoint.sourceIndex(),
            checkpoint.sourceShard(),
            checkpoint.primaryTerm(),
            checkpoint.maxSeqNo(),
            checkpoint.infosVersion(),
            checkpoint.fileMetadata().size()
        );
    }

    /**
     * Atomically consumes the latest checkpoint for the given slot. Returns
     * {@code null} if the mailbox is empty for this slot (triggers pull fallback).
     *
     * <p>On consume, records push-to-consume latency from the checkpoint's
     * createdTimeStampMillis for observability.
     */
    public MVReplicationCheckpoint consume(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        String key = slotKey(targetIndex, targetShard, sourceIndex, sourceShard);
        MVReplicationCheckpoint checkpoint = slots.remove(key);
        if (checkpoint != null) {
            consumeCount.incrementAndGet();
            // Track the last consumed maxSeqNo as the target's effective watermark
            lastConsumedWatermarks.compute(key, (k, existing) -> {
                if (existing == null) return new AtomicLong(checkpoint.maxSeqNo());
                existing.accumulateAndGet(checkpoint.maxSeqNo(), Math::max);
                return existing;
            });
            // Latency observability: push-to-consume delay
            long consumeLatencyMs = System.currentTimeMillis() - checkpoint.createdTimeStampMillis();
            logger.debug(
                "MAILBOX_HIT target=[{}][{}] source=[{}][{}] term={} maxSeqNo={} infosVersion={} consume_latency_ms={}",
                targetIndex,
                targetShard,
                sourceIndex,
                sourceShard,
                checkpoint.primaryTerm(),
                checkpoint.maxSeqNo(),
                checkpoint.infosVersion(),
                consumeLatencyMs
            );
            // Record latency metric
            MVBuildMetrics.INSTANCE.recordCheckpointConsumeLatency(consumeLatencyMs);
        }
        return checkpoint;
    }

    /**
     * Peeks at the latest checkpoint without consuming it. Used for cheap
     * "has the mailbox advanced?" checks in the poller's no-op path.
     */
    public MVReplicationCheckpoint peek(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return slots.get(slotKey(targetIndex, targetShard, sourceIndex, sourceShard));
    }

    /**
     * Returns the last consumed maxSeqNo for a given target/source slot — the
     * target's effective watermark as seen by this mailbox. Returns -1 if the
     * slot has never been consumed. Used by the transport handler to populate
     * the publish response.
     */
    public long lastConsumedWatermark(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        AtomicLong wm = lastConsumedWatermarks.get(slotKey(targetIndex, targetShard, sourceIndex, sourceShard));
        return wm != null ? wm.get() : -1L;
    }

    public long pushCount() {
        return pushCount.get();
    }

    public long consumeCount() {
        return consumeCount.get();
    }

    public int pendingSlots() {
        return slots.size();
    }

    private static String slotKey(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return targetIndex + ":" + targetShard + ":" + sourceIndex + ":" + sourceShard;
    }
}
