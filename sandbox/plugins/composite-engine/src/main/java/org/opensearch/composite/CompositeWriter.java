/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A composite {@link Writer} that wraps one {@link Writer} per registered data format
 * and delegates write operations to each per-format writer.
 * <p>
 * {@code addDoc()} extracts per-format inputs from the {@link CompositeDocumentInput}'s
 * {@code getFinalInput()} map and calls {@code addDoc()} on each per-format writer.
 * {@code flush()} aggregates {@link FileInfos} from all per-format writers.
 * {@code sync()} and {@code close()} delegate to each per-format writer.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeWriter implements Writer<CompositeDocumentInput>, Lock {

    private static final Logger logger = LogManager.getLogger(CompositeWriter.class);
    private final ReentrantLock lock;
    private final Map<DataFormat, Writer<?>> writers;

    /**
     * Constructs a CompositeWriter wrapping the given per-format writers.
     *
     * @param writers a map of data format to its corresponding writer
     */
    public CompositeWriter(Map<DataFormat, Writer<?>> writers) {
        this.writers = Collections.unmodifiableMap(Objects.requireNonNull(writers, "writers must not be null"));
        this.lock = new ReentrantLock();
    }

    @SuppressWarnings("unchecked")
    @Override
    public WriteResult addDoc(CompositeDocumentInput doc) throws IOException {
        Map<DataFormat, Object> perFormatInputs = doc.getFinalInput();
        WriteResult lastResult;
        for (Map.Entry<DataFormat, Writer<?>> entry : writers.entrySet()) {
            DataFormat format = entry.getKey();
            Writer<?> writer = entry.getValue();
            Object input = perFormatInputs.get(format);
            if (input == null) {
                logger.debug("No input found for data format [{}], skipping addDoc", format.name());
                continue;
            }
            lastResult = ((Writer<DocumentInput<?>>) writer).addDoc((DocumentInput<?>) input);

            switch (lastResult) {
                case WriteResult.Success s -> logger.debug("Successfully added document [{}] in [{}]", s, format);
                case WriteResult.Failure f -> {
                    logger.debug("Failed to add document [{}] in [{}]", f, format);
                    return lastResult;
                }
            }
        }

        if (lastResult == null) {
            return new WriteResult.Success(false, null, -1L);
        }
        return lastResult;
    }

    @Override
    public FileInfos flush() throws IOException {
        FileInfos.Builder builder = FileInfos.builder();
        for (Writer<?> writer : writers.values()) {
            FileInfos perFormatInfos = writer.flush();
            builder.putAll(perFormatInfos.getWriterFilesMap());
        }
        return builder.build();
    }

    @Override
    public void sync() throws IOException {
        for (Writer<?> writer : writers.values()) {
            writer.sync();
        }
    }

    @Override
    public void close() {
        for (Map.Entry<DataFormat, Writer<?>> entry : writers.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                logger.warn(new ParameterizedMessage("Failed to close per-format Writer for format [{}]", entry.getKey().name()), e);
            }
        }
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }
}
