/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Target-side ledger of published ship batches per target/source shard pair.
 * The ship handler stages every fully applied batch before target refresh;
 * that refresh promotes claims under the engine refresh lock, making search
 * publication and exact claimability atomic. A target commit is allowed only
 * when the source has asynchronously signaled a committed checkpoint covering
 * the complete published claim.
 *
 * <p>The compatibility cursor is (Gc, s), where Gc is the highest complete
 * recorded generation and s is its contiguous fold floor. Exact source
 * coverage is tracked independently as the union of every complete batch, so
 * above-floor ranges survive publication, commit metadata, and restart. A
 * wholly absent generation number is not a gap: writer generations can skip
 * numbers, while ships arrive in generation order per source shard.
 *
 * <p>Static registry keyed by target coordinates because the ship transport
 * handler and the per-shard engine instance have no shared wiring in the
 * POC plugin. Entries are seeded from the target's own last commit at
 * engine open so the cursor never regresses across restarts.
 */
final class MVTargetCursorLedger {

    /** One applied ship batch. Complete when rowsApplied == rowsExpected. */
    record Batch(long foldCheckpoint, long maxSeqNo, int rowsExpected, int rowsApplied, MVSourceSeqCoverage sourceCoverage) {
        Batch(long foldCheckpoint, long maxSeqNo, int rowsExpected, int rowsApplied) {
            this(foldCheckpoint, maxSeqNo, rowsExpected, rowsApplied, MVSourceSeqCoverage.contiguous(foldCheckpoint));
        }

        boolean complete() {
            return rowsApplied == rowsExpected;
        }
    }

    /** Certified cursor: sweep key Gc + contiguous fold checkpoint s. */
    record Cursor(long certifiedGeneration, long checkpoint) {
        static final Cursor NONE = new Cursor(-1L, -1L);

        String encode() {
            return certifiedGeneration + ":" + checkpoint;
        }

        static Cursor decode(String s) {
            try {
                int i = s.indexOf(':');
                int end = s.indexOf('|', i + 1);
                String checkpoint = end < 0 ? s.substring(i + 1) : s.substring(i + 1, end);
                return new Cursor(Long.parseLong(s.substring(0, i)), Long.parseLong(checkpoint));
            } catch (Exception e) {
                return NONE;
            }
        }
    }

    private record PairKey(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
    }

    static String encodeCommit(Cursor cursor, MVSourceSeqCoverage coverage) {
        return cursor.encode() + "|" + coverage.encode();
    }

    static MVSourceSeqCoverage decodeCommitCoverage(String encoded) {
        int separator = encoded.indexOf('|');
        if (separator < 0) {
            return MVSourceSeqCoverage.contiguous(Cursor.decode(encoded).checkpoint());
        }
        return MVSourceSeqCoverage.decode(encoded.substring(separator + 1));
    }

    private static final ConcurrentMap<PairKey, ConcurrentSkipListMap<Long, Batch>> BATCHES = new ConcurrentHashMap<>();
    /**
     * Batches applied to writers but NOT yet published by a refresh. Pending
     * entries are NEVER counted by {@link #certified} — a commit interleaving
     * before the publishing refresh must not claim them (they are not in its
     * snapshot), and the publishing refresh itself promotes them under the
     * engine's refreshLock, making publication and claimability one event.
     */
    private static final ConcurrentMap<PairKey, ConcurrentSkipListMap<Long, Batch>> PENDING = new ConcurrentHashMap<>();
    /** Floor cursors seeded from the target's own last commit (never regress). */
    private static final ConcurrentMap<PairKey, Cursor> FLOOR = new ConcurrentHashMap<>();
    /** Exact durable claims corresponding to FLOOR, including above-floor ranges. */
    private static final ConcurrentMap<PairKey, MVSourceSeqCoverage> EXACT_FLOOR = new ConcurrentHashMap<>();
    /** Source commit cap: target commit metadata may never claim above this checkpoint. */
    private static final ConcurrentMap<PairKey, Long> SOURCE_COMMIT_CAP = new ConcurrentHashMap<>();

    private MVTargetCursorLedger() {}

    static void advanceSourceCommitCap(String targetIndex, int targetShard, String sourceIndex, int sourceShard, long checkpoint) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        SOURCE_COMMIT_CAP.merge(key, checkpoint, Math::max);
    }

    static long sourceCommitCap(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return SOURCE_COMMIT_CAP.getOrDefault(new PairKey(targetIndex, targetShard, sourceIndex, sourceShard), -1L);
    }

    /**
     * Ship handler: stage a fully-applied batch BEFORE the publishing
     * refresh. Staged batches become claimable only via {@link #promoteAll}.
     */
    static void stagePending(
        String targetIndex,
        int targetShard,
        String sourceIndex,
        int sourceShard,
        long generation,
        long foldCheckpoint,
        long maxSeqNo,
        int rowsExpected,
        int rowsApplied
    ) {
        stagePending(
            targetIndex,
            targetShard,
            sourceIndex,
            sourceShard,
            generation,
            foldCheckpoint,
            maxSeqNo,
            rowsExpected,
            rowsApplied,
            MVSourceSeqCoverage.contiguous(foldCheckpoint)
        );
    }

    static void stagePending(
        String targetIndex,
        int targetShard,
        String sourceIndex,
        int sourceShard,
        long generation,
        long foldCheckpoint,
        long maxSeqNo,
        int rowsExpected,
        int rowsApplied,
        MVSourceSeqCoverage sourceCoverage
    ) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        Batch batch = new Batch(foldCheckpoint, maxSeqNo, rowsExpected, rowsApplied, sourceCoverage);
        PENDING.compute(key, (k, staged) -> {
            ConcurrentSkipListMap<Long, Batch> batches = staged == null ? new ConcurrentSkipListMap<>() : staged;
            batches.put(generation, batch);
            return batches;
        });
    }

    /**
     * Target refresh (under the engine's refreshLock): promote every staged
     * batch for this target shard to RECORDED. Every pending batch's rows
     * are fully in checked-in writers (batch apply is atomic and staged only
     * after full apply), and the publishing refresh drains ALL writers — so
     * promotion here claims exactly what this refresh publishes.
     */
    static void promoteAll(String targetIndex, int targetShard) {
        for (PairKey key : PENDING.keySet()) {
            if (key.targetIndex().equals(targetIndex) == false || key.targetShard() != targetShard) {
                continue;
            }
            PENDING.computeIfPresent(key, (k, staged) -> {
                ConcurrentSkipListMap<Long, Batch> recorded = BATCHES.computeIfAbsent(k, ignored -> new ConcurrentSkipListMap<>());
                recorded.putAll(staged);
                return null;
            });
        }
    }

    /** Ship handler: record a batch after apply+refresh completed. */
    static void record(
        String targetIndex,
        int targetShard,
        String sourceIndex,
        int sourceShard,
        long generation,
        long foldCheckpoint,
        long maxSeqNo,
        int rowsExpected,
        int rowsApplied
    ) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        BATCHES.computeIfAbsent(key, k -> new ConcurrentSkipListMap<>())
            .put(generation, new Batch(foldCheckpoint, maxSeqNo, rowsExpected, rowsApplied));
    }

    static void resetTarget(String targetIndex, int targetShard) {
        BATCHES.keySet().removeIf(key -> key.targetIndex().equals(targetIndex) && key.targetShard() == targetShard);
        PENDING.keySet().removeIf(key -> key.targetIndex().equals(targetIndex) && key.targetShard() == targetShard);
        FLOOR.keySet().removeIf(key -> key.targetIndex().equals(targetIndex) && key.targetShard() == targetShard);
        EXACT_FLOOR.keySet().removeIf(key -> key.targetIndex().equals(targetIndex) && key.targetShard() == targetShard);
        SOURCE_COMMIT_CAP.keySet().removeIf(key -> key.targetIndex().equals(targetIndex) && key.targetShard() == targetShard);
    }

    /** Target engine open: seed the floor so the cursor never regresses. */
    static void seed(String targetIndex, int targetShard, String sourceIndex, int sourceShard, Cursor fromCommit) {
        seed(targetIndex, targetShard, sourceIndex, sourceShard, fromCommit, MVSourceSeqCoverage.contiguous(fromCommit.checkpoint()));
    }

    static void seed(
        String targetIndex,
        int targetShard,
        String sourceIndex,
        int sourceShard,
        Cursor fromCommit,
        MVSourceSeqCoverage exactClaim
    ) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        FLOOR.merge(key, fromCommit, (a, b) -> a.checkpoint() >= b.checkpoint() ? a : b);
        EXACT_FLOOR.merge(key, exactClaim, MVSourceSeqCoverage::union);
        SOURCE_COMMIT_CAP.merge(key, exactClaim.maxClaimedSeqNo(), Math::max);
        BATCHES.remove(key);
        PENDING.remove(key);
    }

    /**
     * The certified cursor for one pairing: floor from the last commit,
     * advanced by the complete prefix of recorded batches.
     */
    static Cursor certified(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        Cursor cursor = FLOOR.getOrDefault(key, Cursor.NONE);
        ConcurrentSkipListMap<Long, Batch> gens = BATCHES.get(key);
        if (gens != null) {
            for (Map.Entry<Long, Batch> e : gens.entrySet()) {
                if (e.getValue().complete() == false) {
                    break; // incomplete batch stops certification (arrival prefix rule)
                }
                long checkpoint = Math.max(e.getValue().foldCheckpoint(), cursor.checkpoint());
                long generation = Math.max(e.getKey(), cursor.certifiedGeneration());
                if (checkpoint > cursor.checkpoint() || generation > cursor.certifiedGeneration()) {
                    cursor = new Cursor(generation, checkpoint);
                }
            }
        }
        return cursor;
    }

    /** Durable cursor from the target's last successful catalog commit. */
    static Cursor committed(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        return FLOOR.getOrDefault(new PairKey(targetIndex, targetShard, sourceIndex, sourceShard), Cursor.NONE);
    }

    /** Exact published claim for one pairing: durable floor plus complete chunk sequence sets. */
    static MVSourceSeqCoverage certifiedCoverage(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        MVSourceSeqCoverage coverage = EXACT_FLOOR.getOrDefault(
            key,
            MVSourceSeqCoverage.contiguous(FLOOR.getOrDefault(key, Cursor.NONE).checkpoint())
        );
        ConcurrentSkipListMap<Long, Batch> batches = BATCHES.get(key);
        if (batches != null) {
            for (Batch batch : batches.values()) {
                if (batch.complete() == false) {
                    break;
                }
                coverage = coverage.union(batch.sourceCoverage());
            }
        }
        return coverage;
    }

    /**
     * Marks a cursor durable after the target catalog commit succeeds. Newer
     * batches that arrived concurrently remain queued for the next commit.
     */
    static void markCommitted(String targetIndex, int targetShard, String sourceIndex, int sourceShard, Cursor cursor) {
        markCommitted(targetIndex, targetShard, sourceIndex, sourceShard, cursor, MVSourceSeqCoverage.contiguous(cursor.checkpoint()));
    }

    static void markCommitted(
        String targetIndex,
        int targetShard,
        String sourceIndex,
        int sourceShard,
        Cursor cursor,
        MVSourceSeqCoverage exactClaim
    ) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        FLOOR.merge(key, cursor, (a, b) -> a.checkpoint() >= b.checkpoint() ? a : b);
        EXACT_FLOOR.merge(key, exactClaim, MVSourceSeqCoverage::union);
        ConcurrentSkipListMap<Long, Batch> batches = BATCHES.get(key);
        if (batches != null) {
            batches.headMap(cursor.certifiedGeneration(), true).clear();
        }
    }

    /** All applied-prefix candidates currently known for a target shard. */
    static Map<String, Cursor> commitCandidatesForTarget(String targetIndex, int targetShard) {
        Map<String, Cursor> out = new java.util.HashMap<>();
        java.util.stream.Stream.concat(BATCHES.keySet().stream(), FLOOR.keySet().stream())
            .filter(k -> k.targetIndex().equals(targetIndex) && k.targetShard() == targetShard)
            .distinct()
            .forEach(
                k -> out.put(
                    k.sourceIndex() + "." + k.sourceShard(),
                    certified(k.targetIndex(), k.targetShard(), k.sourceIndex(), k.sourceShard())
                )
            );
        return out;
    }

    static boolean allPublishedWithinSourceCommitCaps(String targetIndex, int targetShard) {
        java.util.Set<PairKey> keys = new java.util.HashSet<>();
        keys.addAll(BATCHES.keySet());
        keys.addAll(FLOOR.keySet());
        for (PairKey key : keys) {
            if (key.targetIndex().equals(targetIndex) == false || key.targetShard() != targetShard) {
                continue;
            }
            ConcurrentSkipListMap<Long, Batch> batches = BATCHES.get(key);
            if (batches != null && batches.values().stream().anyMatch(batch -> batch.complete() == false)) {
                return false;
            }
            MVSourceSeqCoverage published = certifiedCoverage(targetIndex, targetShard, key.sourceIndex(), key.sourceShard());
            if (published.maxClaimedSeqNo() > sourceCommitCap(targetIndex, targetShard, key.sourceIndex(), key.sourceShard())) {
                return false;
            }
        }
        return true;
    }

    /** Exact durable source claim from the target's last successful catalog commit. */
    static MVSourceSeqCoverage committedCoverage(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
        PairKey key = new PairKey(targetIndex, targetShard, sourceIndex, sourceShard);
        return EXACT_FLOOR.getOrDefault(key, MVSourceSeqCoverage.contiguous(FLOOR.getOrDefault(key, Cursor.NONE).checkpoint()));
    }

    static boolean allAppliedCommitted(String targetIndex, int targetShard) {
        java.util.Set<PairKey> keys = new java.util.HashSet<>();
        keys.addAll(BATCHES.keySet());
        keys.addAll(FLOOR.keySet());
        for (PairKey key : keys) {
            if (key.targetIndex().equals(targetIndex) == false || key.targetShard() != targetShard) {
                continue;
            }
            ConcurrentSkipListMap<Long, Batch> batches = BATCHES.get(key);
            if (batches != null && batches.values().stream().anyMatch(batch -> batch.complete() == false)) {
                return false;
            }
            MVSourceSeqCoverage published = certifiedCoverage(targetIndex, targetShard, key.sourceIndex(), key.sourceShard());
            MVSourceSeqCoverage durable = committedCoverage(targetIndex, targetShard, key.sourceIndex(), key.sourceShard());
            if (published.equals(durable) == false) {
                return false;
            }
        }
        return true;
    }

    static void clearForTests() {
        BATCHES.clear();
        PENDING.clear();
        FLOOR.clear();
        EXACT_FLOOR.clear();
        SOURCE_COMMIT_CAP.clear();
    }
}
