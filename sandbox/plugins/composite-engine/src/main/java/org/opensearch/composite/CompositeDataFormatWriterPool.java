/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.composite.queue.LockableConcurrentQueue;

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

public class CompositeWriterPool implements Iterable<CompositeWriter>, Closeable {

    private final Set<CompositeWriter> writers;
    private final LockableConcurrentQueue<CompositeWriter> availableWriters;
    private final Supplier<CompositeWriter> writerSupplier;
    private volatile boolean closed;

    public CompositeWriterPool(
        Supplier<CompositeWriter> writerSupplier,
        Supplier<Queue<CompositeWriter>> queueSupplier,
        int concurrency
    ) {
        this.writers = Collections.newSetFromMap(new IdentityHashMap<>());
        this.writerSupplier = writerSupplier;
        this.availableWriters = new LockableConcurrentQueue<>(queueSupplier, concurrency);
    }

    /**
     * This method is used by CompositeIndexingExecutionEngine to grab a writer from the pool to perform an indexing
     * operation.
     *
     * @return a pooled CompositeWriter if available, or a newly created instance if none are available
     */
    public CompositeWriter getAndLock() {
        ensureOpen();
        CompositeWriter CompositeWriter = availableWriters.lockAndPoll();
        return Objects.requireNonNullElseGet(CompositeWriter, this::fetchWriter);
    }

    /**
     * Create a new {@link CompositeWriter} to be added to this pool.
     *
     * @return a new instance of {@link CompositeWriter}
     */
    private synchronized CompositeWriter fetchWriter() {
        ensureOpen();
        CompositeWriter CompositeWriter = writerSupplier.get();
        CompositeWriter.lock();
        writers.add(CompositeWriter);
        return CompositeWriter;
    }

    /**
     * Release the given {@link CompositeWriter} to this pool for reuse if it is currently managed by this
     * pool.
     *
     * @param state {@link CompositeWriter} to release to the pool.
     */
    public void releaseAndUnlock(CompositeWriter state) {
        assert
            !state.isFlushPending() && !state.isAborted() :
            "CompositeWriter has pending flush: " + state.isFlushPending() + " aborted=" + state.isAborted();
        assert isRegistered(state) : "CompositeDocumentWriterPool doesn't know about this CompositeWriter";
        availableWriters.addAndUnlock(state);
    }

    /**
     * Lock and checkout all CompositeWriters from the pool for flush.
     *
     * @return Unmodifiable list of all CompositeWriters locked by current thread.
     */
    public List<CompositeWriter> checkoutAll() {
        ensureOpen();
        List<CompositeWriter> lockedWriters = new ArrayList<>();
        List<CompositeWriter> checkedOutWriters = new ArrayList<>();
        for (CompositeWriter CompositeWriter : this) {
            CompositeWriter.lock();
            lockedWriters.add(CompositeWriter);
        }
        synchronized (this) {
            for (CompositeWriter CompositeWriter : lockedWriters) {
                try {
                    // Release this writer if it’s no longer managed by this pool; otherwise, check it out.
                    if (isRegistered(CompositeWriter) && writers.remove(CompositeWriter)) {
                        availableWriters.remove(CompositeWriter);
                        CompositeWriter.setFlushPending();
                        checkedOutWriters.add(CompositeWriter);
                    }
                } finally {
                    CompositeWriter.unlock();
                }
            }
        }
        return Collections.unmodifiableList(checkedOutWriters);
    }

    /**
     * Check if {@link CompositeWriter} is part of this pool.
     *
     * @param perThread {@link CompositeWriter} to validate.
     * @return true if {@link CompositeWriter} is part of this pool, false otherwise.
     */
    synchronized boolean isRegistered(CompositeWriter perThread) {
        return writers.contains(perThread);
    }

    private void ensureOpen() {
        if (closed) {
            throw new AlreadyClosedException("CompositeDocumentWriterPool is already closed");
        }
    }

    @Override
    public synchronized Iterator<CompositeWriter> iterator() {
        return List.copyOf(writers).iterator();
    }

    @Override
    public void close() throws IOException {
        this.closed = true;
    }
}

