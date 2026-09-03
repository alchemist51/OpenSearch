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
import org.opensearch.core.index.shard.ShardId;

import java.util.Collections;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-shard tracker for seqNos that consume a sequence number but do NOT produce
 * a parquet row. These "noop" seqNos include:
 * <ul>
 *   <li>Index operations that FAIL after seqNo assignment on the primary
 *       (e.g. mapping parse failures) — {@link org.opensearch.index.engine.Engine.Result.Type#FAILURE}
 *       with an assigned seqNo</li>
 *   <li>Delete operations — all deletes consume a seqNo but never produce
 *       a parquet row regardless of success/failure</li>
 * </ul>
 *
 * <p>The tracker is populated by an {@link org.opensearch.index.shard.IndexingOperationListener}
 * registered on source indices via {@code onIndexModule}. It is consulted by
 * {@link MVReplicationService} when building checkpoints and by
 * {@link MVCheckpointRequestTransportHandler} for checkpoint responses.</p>
 *
 * <h2>Thread safety</h2>
 * <p>Per-shard sets are {@link ConcurrentSkipListSet} — lock-free concurrent
 * sorted structures. Multiple indexing threads may record concurrently; the
 * checkpoint tick reads a consistent snapshot of the range it needs.</p>
 *
 * <h2>Eviction</h2>
 * <p>SeqNos at or below the minimum confirmed target watermark are no longer
 * needed (all targets have processed past that point). Eviction is triggered
 * by {@link #evictBelow(ShardId, long)} called from the replication tick after
 * reading publisher watermarks. Additionally, a per-shard size cap (default
 * {@value #DEFAULT_MAX_TRACKED_PER_SHARD}) prevents unbounded growth if
 * targets are very far behind — the oldest entries are evicted with a WARN.</p>
 *
 * <h2>Restart limitation</h2>
 * <p>The tracker is in-memory only. After service restart, seqNos consumed
 * before the service started have unknown noop status. The coverage check on
 * the target side must account for this by treating unknown-noop-status ranges
 * conservatively (the first round after restart may need to re-derive from
 * the committed watermark). See TODO in the class for Lucene soft-delete
 * tombstone fallback investigation notes.</p>
 *
 * @opensearch.experimental
 */
public final class MVNoopTracker {

    private static final Logger logger = LogManager.getLogger(MVNoopTracker.class);

    /**
     * Default maximum tracked noop seqNos per shard. When exceeded, the oldest
     * entries are evicted to stay within budget. This is a safety cap — normal
     * operation with timely target consumption keeps the set small.
     */
    static final int DEFAULT_MAX_TRACKED_PER_SHARD = 100_000;

    private final int maxTrackedPerShard;

    /** Per-shard sorted set of noop seqNos. */
    private final ConcurrentHashMap<ShardId, ConcurrentSkipListSet<Long>> shardNoops = new ConcurrentHashMap<>();

    /** Metrics */
    private final AtomicLong totalRecorded = new AtomicLong();
    private final AtomicLong totalEvicted = new AtomicLong();
    private final AtomicLong capEvictions = new AtomicLong();

    public MVNoopTracker() {
        this(DEFAULT_MAX_TRACKED_PER_SHARD);
    }

    public MVNoopTracker(int maxTrackedPerShard) {
        this.maxTrackedPerShard = maxTrackedPerShard;
    }

    /**
     * Records a seqNo as a noop (consumed a sequence number but produced no parquet row).
     * Idempotent — duplicate seqNos are silently absorbed by the set.
     *
     * @param shardId the source shard that consumed this seqNo
     * @param seqNo   the sequence number to record
     */
    public void recordNoop(ShardId shardId, long seqNo) {
        if (seqNo < 0) return; // UNASSIGNED_SEQ_NO or invalid — ignore
        ConcurrentSkipListSet<Long> noops = shardNoops.computeIfAbsent(shardId, k -> new ConcurrentSkipListSet<>());
        noops.add(seqNo);
        totalRecorded.incrementAndGet();

        // Size cap eviction: remove oldest entries if over budget
        if (noops.size() > maxTrackedPerShard) {
            int excess = noops.size() - maxTrackedPerShard;
            for (int i = 0; i < excess; i++) {
                Long removed = noops.pollFirst();
                if (removed != null) {
                    totalEvicted.incrementAndGet();
                    capEvictions.incrementAndGet();
                }
            }
            if (excess > 0) {
                logger.warn(
                    "mv_noop_tracker: shard [{}] exceeded cap {} — evicted {} oldest entries. "
                        + "Targets may be too far behind.",
                    shardId,
                    maxTrackedPerShard,
                    excess
                );
            }
        }
    }

    /**
     * Returns the noop seqNos in the half-open range (fromExclusive, toInclusive]
     * for the given shard. Returns an empty set if no noops are tracked for that
     * range or shard.
     *
     * <p>The returned array is sorted in ascending order and is a snapshot —
     * concurrent modifications do not affect it.</p>
     *
     * @param shardId       the source shard
     * @param fromExclusive lower bound (exclusive); -1 means "from the beginning"
     * @param toInclusive   upper bound (inclusive)
     * @return sorted array of noop seqNos in the range, never null
     */
    public long[] getNoopsInRange(ShardId shardId, long fromExclusive, long toInclusive) {
        ConcurrentSkipListSet<Long> noops = shardNoops.get(shardId);
        if (noops == null || noops.isEmpty()) {
            return EMPTY;
        }
        // subSet(fromExclusive+1, true, toInclusive, true) gives us (fromExclusive, toInclusive]
        NavigableSet<Long> subset = noops.subSet(fromExclusive + 1, true, toInclusive, true);
        if (subset.isEmpty()) {
            return EMPTY;
        }
        return subset.stream().mapToLong(Long::longValue).toArray();
    }

    /**
     * Evicts all noop seqNos at or below the given watermark for the specified shard.
     * Called when the minimum target watermark advances — those seqNos are no longer
     * needed by any target.
     *
     * @param shardId  the source shard
     * @param watermark evict seqNos at or below this value
     * @return number of entries evicted
     */
    public int evictBelow(ShardId shardId, long watermark) {
        ConcurrentSkipListSet<Long> noops = shardNoops.get(shardId);
        if (noops == null || noops.isEmpty() || watermark < 0) {
            return 0;
        }
        int count = 0;
        // headSet(watermark, true) gives us all entries <= watermark
        NavigableSet<Long> toRemove = noops.headSet(watermark, true);
        // ConcurrentSkipListSet.headSet returns a view — iterate and remove
        java.util.Iterator<Long> iter = toRemove.iterator();
        while (iter.hasNext()) {
            iter.next();
            iter.remove();
            count++;
        }
        if (count > 0) {
            totalEvicted.addAndGet(count);
        }
        return count;
    }

    /**
     * Removes all tracked data for a shard (shard closed/removed).
     */
    public void removeShard(ShardId shardId) {
        shardNoops.remove(shardId);
    }

    /**
     * Returns the number of currently tracked noop seqNos for a shard.
     */
    public int trackedCount(ShardId shardId) {
        ConcurrentSkipListSet<Long> noops = shardNoops.get(shardId);
        return noops != null ? noops.size() : 0;
    }

    /**
     * Returns the total number of shards currently tracked.
     */
    public int trackedShardCount() {
        return shardNoops.size();
    }

    public long totalRecorded() {
        return totalRecorded.get();
    }

    public long totalEvicted() {
        return totalEvicted.get();
    }

    public long capEvictions() {
        return capEvictions.get();
    }

    private static final long[] EMPTY = new long[0];

    /*
     * TODO: Restart gap-filling via Lucene soft-delete tombstones.
     *
     * After service restart, the tracker is empty — noop seqNos from before the
     * restart are unknown. A potential gap-filling strategy:
     *
     * 1. On first tick after restart (when tracker is empty for a tracked shard),
     *    acquire a searcher via shard.acquireSearcher("mv_noop_recovery")
     * 2. Query for soft-delete tombstones: filter on _tombstone=1 or docs where
     *    _soft_deletes field is set, reading _seq_no from stored fields
     * 3. Delete tombstones carry seqNos but no indexed content — these are noops
     *    from the MV perspective
     * 4. Also detect failed index ops via Lucene's soft-deletes where the doc
     *    exists as a tombstone with no real content
     *
     * Challenges:
     * - Soft-delete retention policy may have already GC'd old tombstones
     * - The query cost scales with tombstone count (could be expensive on
     *   high-delete workloads)
     * - Would need to intersect with the current round range to be useful
     *
     * For V1, the restart limitation is documented and the first post-restart
     * round operates conservatively. A full implementation of tombstone-based
     * recovery is deferred to a follow-up.
     */
}
