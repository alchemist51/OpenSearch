/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A thread-safe pool of {@link Lockable} items backed by a {@link LockableConcurrentQueue}.
 * Items are locked on checkout and unlocked on release, ensuring safe reuse across threads.
 * <p>
 * The pool is created with a supplier that produces new items on demand when the pool
 * is empty. Items are tracked in a set for registration checks and iteration.
 *
 * @param <T> the pooled item type, must implement {@link Lockable}
 */
public final class LockablePool<T extends Lockable> implements Iterable<T>, Closeable {

    private volatile Set<T> items;
    private volatile LockableConcurrentQueue<T> availableItems;
    private final Supplier<T> itemSupplier;
    private final Supplier<Queue<T>> queueSupplier;
    private final int concurrency;
    private volatile boolean closed;

    /**
     * Creates a new pool.
     *
     * @param itemSupplier  factory for creating new items when the pool is empty
     * @param queueSupplier supplier for the underlying queue instances
     * @param concurrency   the concurrency level (number of stripes)
     */
    public LockablePool(Supplier<T> itemSupplier, Supplier<Queue<T>> queueSupplier, int concurrency) {
        this.items = Collections.newSetFromMap(new IdentityHashMap<>());
        this.itemSupplier = Objects.requireNonNull(itemSupplier, "itemSupplier must not be null");
        this.queueSupplier = Objects.requireNonNull(queueSupplier, "queueSupplier must not be null");
        this.concurrency = concurrency;
        this.availableItems = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * Locks and polls an item from the pool, or creates a new one if none are available.
     *
     * @return a locked item
     * @throws IllegalStateException if the pool is closed
     */
    public T getAndLock() {
        ensureOpen();
        T item = availableItems.lockAndPoll();
        return Objects.requireNonNullElseGet(item, this::fetchItem);
    }

    private synchronized T fetchItem() {
        ensureOpen();
        T item = itemSupplier.get();
        item.lock();
        items.add(item);
        return item;
    }

    /**
     * Releases the given item back to this pool for reuse. If the item belongs to a previous
     * generation (swapped out during {@link #checkoutAll()}), it is silently unlocked and
     * discarded since the checkout caller owns it.
     *
     * @param item the item to release
     */
    public void releaseAndUnlock(T item) {
        if (isRegistered(item) == false) {
            // Item belongs to a previous generation swapped out during checkoutAll().
            // Just unlock it — the checkout caller owns it now.
            item.unlock();
            return;
        }
        availableItems.addAndUnlock(item);
    }

    /**
     * Atomically swaps the pool's item set and queue with fresh instances, then waits for
     * any in-flight operations on the old items to complete. This minimizes the time the pool
     * lock is held — callers of {@link #getAndLock()} see the new empty pool immediately and
     * can create fresh items without waiting for the checkout to finish.
     *
     * @return unmodifiable list of all checked-out items
     * @throws IllegalStateException if the pool is closed
     */
    public List<T> checkoutAll() {
        ensureOpen();

        // Step 1: Atomic swap — hold pool lock only for the reference swap.
        Set<T> oldItems;
        synchronized (this) {
            oldItems = this.items;
            this.items = Collections.newSetFromMap(new IdentityHashMap<>());
            this.availableItems = new LockableConcurrentQueue<>(queueSupplier, concurrency);
        }
        // Pool lock released — concurrent getAndLock() calls proceed immediately with fresh pool.

        // Step 2: Wait for in-flight operations on old items to complete.
        // No pool lock held here, so no contention with concurrent callers.
        List<T> checkedOutItems = new ArrayList<>(oldItems.size());
        for (T item : oldItems) {
            item.lock();
            try {
                checkedOutItems.add(item);
            } finally {
                item.unlock();
            }
        }
        return Collections.unmodifiableList(checkedOutItems);
    }

    /**
     * Check if an item is part of this pool.
     *
     * @param item item to validate
     * @return true if the item is part of this pool
     */
    public synchronized boolean isRegistered(T item) {
        return items.contains(item);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("LockablePool is already closed");
        }
    }

    @Override
    public synchronized Iterator<T> iterator() {
        return List.copyOf(items).iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}
