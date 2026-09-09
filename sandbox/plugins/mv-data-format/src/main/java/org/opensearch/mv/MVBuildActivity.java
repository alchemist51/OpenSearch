/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-target-shard signal that an MV build round is currently holding native
 * pool memory (defect #23: build/merge shared-pool contention).
 *
 * <p>Direction of stall is a design decision for MVs: <b>merges stall, live
 * ingestion never does</b> — the inverse of Lucene's stall (which throttles
 * indexing when merges lag). merge-admission gating consults
 * this signal, so while a build round is active BOTH selection paths (scheduled
 * and force) admit no new merges for the shard. Deferred candidates need no
 * queue: every publication re-triggers the scheduler, and the final round of a
 * catch-up burst clears its claim <i>before</i> publishing — so the transition
 * to quiet is exactly the trigger that re-admits merges.
 *
 * <p>Claims are counted, not boolean, and the builder pairs mark/clear in
 * try/finally — the same "every claim resolves" invariant the merge scheduler
 * maintains for its registered merges (defect #22).
 *
 * <p>Static registry keyed by target coordinates for the same reason as
 * the deleted cursor ledger: the pull-path builder and merge admission
 * instance have no shared wiring in the POC plugin.
 */
public final class MVBuildActivity {

    private static final ConcurrentMap<String, AtomicInteger> ACTIVE = new ConcurrentHashMap<>();

    private MVBuildActivity() {}

    private static String key(String targetIndex, int targetShard) {
        return targetIndex + ":" + targetShard;
    }

    /** Builder: a build round is entering its memory-holding section. */
    public static void markActive(String targetIndex, int targetShard) {
        ACTIVE.computeIfAbsent(key(targetIndex, targetShard), k -> new AtomicInteger()).incrementAndGet();
    }

    /** Builder: the round released its memory (success, failure, or pre-publish handoff). */
    public static void clearActive(String targetIndex, int targetShard) {
        ACTIVE.computeIfPresent(key(targetIndex, targetShard), (k, count) -> {
            int now = count.decrementAndGet();
            if (now < 0) {
                count.set(0); // defensive floor: unmatched clear must not poison future marks
                now = 0;
            }
            return now == 0 ? null : count;
        });
    }

    /** Engine merge-eligibility: is a build round holding memory for this shard right now? */
    public static boolean isActive(String targetIndex, int targetShard) {
        AtomicInteger count = ACTIVE.get(key(targetIndex, targetShard));
        return count != null && count.get() > 0;
    }

    public static void clearForTests() {
        ACTIVE.clear();
    }
}
