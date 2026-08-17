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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * POC(mv) forward buffer (v2): VSR with the MV's referenced columns —
 * service, status (utf8) and latency_ms (int64). Rotation exports via Arrow
 * C Data and feeds the native DataFusion-maintained state.
 *
 * <p>Rollback contract unchanged: rotation happens BEFORE appending, so a
 * failed doc is always still in this buffer; truncateTo undoes it.
 */
final class MVForwardBuffer implements AutoCloseable {

    static final int ROTATION_THRESHOLD = 4096;

    private final BufferAllocator allocator;
    private final VectorSchemaRoot vsr;
    private final VarCharVector serviceVector;
    private final VarCharVector statusVector;
    private final BigIntVector latencyVector;
    private int rowCount = 0;

    MVForwardBuffer() {
        this.allocator = new RootAllocator(64L * 1024 * 1024);
        this.serviceVector = new VarCharVector("service", allocator);
        this.statusVector = new VarCharVector("status", allocator);
        this.latencyVector = new BigIntVector("latency_ms", allocator);
        this.vsr = new VectorSchemaRoot(List.of(serviceVector, statusVector, latencyVector));
    }

    void append(MVDocumentInput.Row row) {
        setVarChar(serviceVector, rowCount, row.service());
        setVarChar(statusVector, rowCount, row.status());
        if (row.latencyMs() == null) {
            latencyVector.setNull(rowCount);
        } else {
            latencyVector.setSafe(rowCount, row.latencyMs());
        }
        rowCount++;
    }

    private static void setVarChar(VarCharVector v, int idx, String value) {
        if (value == null) {
            v.setNull(idx);
        } else {
            v.setSafe(idx, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    int rowCount() {
        return rowCount;
    }

    boolean shouldRotate() {
        return rowCount >= ROTATION_THRESHOLD;
    }

    void truncateTo(int rows) {
        rowCount = rows;
    }

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

    /** The buffer's allocator — also used to import the finalized state batch for the ship path. */
    org.apache.arrow.memory.BufferAllocator allocator() {
        return allocator;
    }
}
