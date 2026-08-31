/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reference-counted ownership of one finalized state batch shared across
 * MULTIPLE ship targets.
 *
 * <p>The batch is the live Arrow root the native writer finalized into —
 * one copy in memory, read-only from every consumer's perspective (concurrent
 * reads of immutable Arrow buffers are safe). No single destination may close
 * it: the SOURCE acquires one reference per target before shipping, each
 * target's handler {@link #release()}s exactly once when done (success or
 * failure), and the source releases a target's reference itself when the ship
 * to that target fails before the handler could take ownership. The LAST
 * release closes the root, which fires the C-Data release callback and frees
 * the native allocation.
 *
 * <p>Releases past zero throw — a double release is an ownership bug, not a
 * condition to tolerate silently (the Arrow debug allocator in tests catches
 * the complementary leak case of a missing release).
 */
public final class MVRefCountedStateBatch {

    private final VectorSchemaRoot root;
    private final AtomicInteger refs;

    /**
     * @param root the finalized state batch; ownership transfers to this holder
     * @param consumers number of references handed out (one per ship target)
     */
    public MVRefCountedStateBatch(VectorSchemaRoot root, int consumers) {
        if (consumers <= 0) {
            throw new IllegalArgumentException("consumers must be positive, got " + consumers);
        }
        this.root = root;
        this.refs = new AtomicInteger(consumers);
    }

    /** Read-only access to the shared batch. Valid until the caller's own {@link #release()}. */
    public VectorSchemaRoot root() {
        return root;
    }

    /** Releases one reference; the last release closes the root (frees native memory). */
    public void release() {
        int remaining = refs.decrementAndGet();
        if (remaining == 0) {
            root.close();
        } else if (remaining < 0) {
            throw new IllegalStateException("mv state batch released more times than acquired");
        }
    }
}
