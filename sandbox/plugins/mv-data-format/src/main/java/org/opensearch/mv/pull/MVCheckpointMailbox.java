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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-global checkpoint mailbox: receives pushed adverts from source shards
 * and delivers them to the target shard's {@link MVDerivedSourceReader}.
 *
 * <p>Each slot is keyed by {@code targetIndex:targetShard:sourceIndex:sourceShard}
 * and holds exactly one (latest) advert. Pushes are coalesced — a newer advert
 * overwrites the previous one. The poller's {@code fetchSnapshot} call
 * atomically consumes (reads and clears) the slot.
 *
 * <p>Thread safety: all operations are lock-free via {@link ConcurrentHashMap}
 * and volatile reads. Multiple source shards may push concurrently; the poller
 * reads on the GENERIC thread pool.
 */
public final class MVCheckpointMailbox {

    private static final Logger logger = LogManager.getLogger(MVCheckpointMailbox.class);

    /** Singleton instance — one per node (created at plugin component creation). */
    private static volatile MVCheckpointMailbox INSTANCE;

    private final ConcurrentMap<String, PushedAdvert> slots = new ConcurrentHashMap<>();
    private final AtomicLong pushCount = new AtomicLong();
    private final AtomicLong consumeCount = new AtomicLong();
    private final AtomicLong fallbackCount = new AtomicLong();
    /**
     * Per-slot last consumed maxSeqNo — the target's effective watermark as
     * seen by the mailbox. Updated on every {@link #consume} call. Key format
     * matches {@link #slotKey}. Returned to the source via the publish response
     * to enable source-side file scoping.
     */
    private final ConcurrentMap<String, AtomicLong> lastConsumedWatermarks = new ConcurrentHashMap<>();

    /** Pushed source advert carrying the metadata the poller needs. */
    public record PushedAdvert(
        String sourceIndex,
        String sourceUuid,
        int sourceShard,
        long maxSeqNo,
        long primaryTerm,
        long infosVersion,
        List<String> parquetFiles,
        List<Long> fileSizes,
        List<Long> fileMinSeqNos,
        List<Long> fileMaxSeqNos,
        long receivedAtNanos
    ) {
        /**
         * Legacy constructor: no per-file seq ranges (all -1).
         */
        public PushedAdvert(
            String sourceIndex,
            String sourceUuid,
            int sourceShard,
            long maxSeqNo,
            long primaryTerm,
            long infosVersion,
            List<String> parquetFiles,
            List<Long> fileSizes,
            long receivedAtNanos
        ) {
            this(sourceIndex, sourceUuid, sourceShard, maxSeqNo, primaryTerm, infosVersion,
                parquetFiles, fileSizes,
                parquetFiles.stream().map(f -> -1L).toList(),
                parquetFiles.stream().map(f -> -1L).toList(),
                receivedAtNanos);
        }
    }

    public MVCheckpointMailbox() {}

    public static MVCheckpointMailbox instance() {
        return INSTANCE;
    }

    public static void setInstance(MVCheckpointMailbox instance) {
        INSTANCE = instance;
    }

    /**
     * Delivers a pushed advert from a source shard. Overwrites any existing
     * advert for the same slot if the new one has a higher maxSeqNo (coalesce).
     */
    public void deliver(String targetIndex, int targetShard, PushedAdvert advert) {
        String key = slotKey(targetIndex, targetShard, advert.sourceIndex(), advert.sourceShard());
        slots.merge(key, advert, (existing, incoming) -> {
            // Keep the advert with the higher maxSeqNo (newer generation)
            if (incoming.maxSeqNo() > existing.maxSeqNo()) {
                return incoming;
            }
            // On tie, prefer higher infosVersion (newer segment generation)
            if (incoming.maxSeqNo() == existing.maxSeqNo() && incoming.infosVersion() > existing.infosVersion()) {
                return incoming;
            }
            return existing;
        });
        pushCount.incrementAndGet();
        logger.debug(
            "PUSH_CHECKPOINT_RECEIVED target=[{}][{}] source=[{}][{}] maxSeqNo={} infosVersion={} files={}",
            targetIndex,
            targetShard,
            advert.sourceIndex(),
            advert.sourceShard(),
            advert.maxSeqNo(),
            advert.infosVersion(),
            advert.parquetFiles().size()
        );
    }

    /**
     * Atomically consumes the latest advert for the given slot. Returns
     * {@code null} if the mailbox is empty for this slot (triggers pull fallback).
     */
    public PushedAdvert consume(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        String key = slotKey(targetIndex, targetShard, sourceIndex, sourceShard);
        PushedAdvert advert = slots.remove(key);
        if (advert != null) {
            consumeCount.incrementAndGet();
            // Track the last consumed maxSeqNo as the target's effective watermark
            lastConsumedWatermarks.compute(key, (k, existing) -> {
                if (existing == null) return new AtomicLong(advert.maxSeqNo());
                existing.accumulateAndGet(advert.maxSeqNo(), Math::max);
                return existing;
            });
            logger.debug(
                "MAILBOX_HIT target=[{}][{}] source=[{}][{}] maxSeqNo={} infosVersion={} age_ms={}",
                targetIndex,
                targetShard,
                sourceIndex,
                sourceShard,
                advert.maxSeqNo(),
                advert.infosVersion(),
                (System.nanoTime() - advert.receivedAtNanos()) / 1_000_000
            );
        }
        return advert;
    }

    /**
     * Peeks at the latest advert without consuming it. Used for cheap
     * "has the mailbox advanced?" checks in the poller's no-op path.
     */
    public PushedAdvert peek(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return slots.get(slotKey(targetIndex, targetShard, sourceIndex, sourceShard));
    }

    /** Records a pull fallback for observability. */
    public void recordFallback() {
        fallbackCount.incrementAndGet();
    }

    /**
     * Returns the last consumed maxSeqNo for a given target/source slot — the
     * target's effective watermark as seen by this mailbox. Returns -1 if the
     * slot has never been consumed (target poller hasn't run yet, or no adverts
     * have been delivered for this slot). Used by the transport handler to
     * populate the publish response.
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

    public long fallbackCount() {
        return fallbackCount.get();
    }

    public int pendingSlots() {
        return slots.size();
    }

    private static String slotKey(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return targetIndex + ":" + targetShard + ":" + sourceIndex + ":" + sourceShard;
    }
}
