/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * POC(mv) forward buffer: a small VSR holding only the MV's referenced
 * columns (here: {@code service}). Rows append during indexing; at rotation
 * the batch exports via Arrow C Data and feeds the native background state.
 *
 * <p>Rollback contract: rotation happens BEFORE appending a new doc
 * (see {@link MVWriter#addDoc}), so a failed doc is always still in this
 * buffer — {@code truncateTo} undoes it without touching folded state.
 */
final class MVForwardBuffer implements AutoCloseable {

    static final int ROTATION_THRESHOLD = 4096;

    private final BufferAllocator allocator;
    private final VectorSchemaRoot vsr;
    private final VarCharVector serviceVector;
    private int rowCount = 0;

    MVForwardBuffer() {
        // POC: private allocator; production enrolls in the shared Arrow allocator.
        this.allocator = new RootAllocator(64L * 1024 * 1024);
        this.serviceVector = new VarCharVector(MVConstants.GROUP_KEY, allocator);
        this.vsr = new VectorSchemaRoot(List.of(serviceVector));
    }

    void append(String service) {
        if (service == null) {
            serviceVector.setNull(rowCount);
        } else {
            serviceVector.setSafe(rowCount, service.getBytes(StandardCharsets.UTF_8));
        }
        rowCount++;
    }

    int rowCount() {
        return rowCount;
    }

    boolean shouldRotate() {
        return rowCount >= ROTATION_THRESHOLD;
    }

    /** Undo appended-but-not-folded rows (rollback path). */
    void truncateTo(int rows) {
        rowCount = rows;
    }

    /**
     * Exports the buffered rows and feeds them into the native background
     * state, then resets the buffer. No-op for an empty buffer.
     */
    void rotateInto(long writerHandle) {
        if (rowCount == 0) {
            return;
        }
        vsr.setRowCount(rowCount);
        try (ArrowArray array = ArrowArray.allocateNew(allocator); ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
            Data.exportVectorSchemaRoot(allocator, vsr, null, array, schema);
            MVNativeBridge.writerFeed(writerHandle, array.memoryAddress(), schema.memoryAddress());
        }
        vsr.clear();
        rowCount = 0;
    }

    @Override
    public void close() {
        vsr.close();
        allocator.close();
    }
}
