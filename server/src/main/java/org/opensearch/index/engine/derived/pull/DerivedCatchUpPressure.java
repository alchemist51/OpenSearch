/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Node-wide signal that one or more derived-pull shards are in sustained
 * catch-up (defect #26: source-merge admission starves derived builds on the
 * shared native memory pool).
 *
 * <p>The stall direction is the same design decision as the per-shard build
 * gate (defect #23): <b>merges stall, derived builds and live ingestion never
 * do</b>. While any poller is catching up — its rounds are capped or failing —
 * {@link org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy}
 * admits no new merges for ANY data-format shard on the node, source and
 * derived alike. The pool is node-global, so per-shard gating cannot protect a
 * derived build from another index's merges; this signal can. In-flight merges
 * are never aborted — they drain, freeing the pool for the starved build.
 *
 * <p>Steady state never claims: a poller holds pressure only after observing a
 * capped or failed round, and releases as soon as a round completes caught-up
 * (or finds no new data). Transient contention outside catch-up self-heals via
 * the poller's fail-closed retry, as proven at small scale.
 *
 * <p>Re-admission needs no new mechanism for busy shards — every publication
 * already re-triggers the scheduler. Idle shards (e.g. a source index whose
 * ingestion finished while its merge backlog was deferred) would strand
 * without a trigger, the same idle-stranding pattern the merge scheduler fixes
 * for registered claims — so engines register an on-release listener that
 * fires their property-gated merge trigger when pressure drops to zero.
 *
 * <p>Claims are counted (one per catching-up poller) and always resolve: the
 * poller pairs hold/release across round outcomes and {@code close()} — the
 * "every claim resolves" invariant shared with defect #22's registered merges
 * and #23's build-round claims.
 */
public final class DerivedCatchUpPressure {

    private static final Logger logger = LogManager.getLogger(DerivedCatchUpPressure.class);

    private static final AtomicInteger CLAIMS = new AtomicInteger();
    private static final List<Runnable> ON_RELEASE = new CopyOnWriteArrayList<>();

    private DerivedCatchUpPressure() {}

    /** Poller: sustained catch-up observed (capped or failed round). */
    public static void claim() {
        int now = CLAIMS.incrementAndGet();
        if (now == 1) {
            logger.info("DERIVED_CATCHUP_PRESSURE held — deferring new data-format merge admission node-wide");
        }
    }

    /** Poller: caught up (or closed). Fires release listeners on the last claim. */
    public static void release() {
        int now = CLAIMS.decrementAndGet();
        if (now < 0) {
            CLAIMS.set(0); // defensive floor: unmatched release must not poison future claims
            return;
        }
        if (now == 0) {
            logger.info("DERIVED_CATCHUP_PRESSURE released — re-admitting deferred data-format merges");
            for (Runnable listener : ON_RELEASE) {
                try {
                    listener.run();
                } catch (Exception e) {
                    logger.warn("derived catch-up pressure release listener failed", e);
                }
            }
        }
    }

    /** Merge admission: is any derived shard catching up right now? */
    public static boolean isActive() {
        return CLAIMS.get() > 0;
    }

    /** Engines register their merge trigger to re-admit deferred merges at release. */
    public static void addOnReleaseListener(Runnable listener) {
        ON_RELEASE.add(listener);
    }

    /** Engines deregister on close. */
    public static void removeOnReleaseListener(Runnable listener) {
        ON_RELEASE.remove(listener);
    }

    public static void clearForTests() {
        CLAIMS.set(0);
        ON_RELEASE.clear();
    }
}
