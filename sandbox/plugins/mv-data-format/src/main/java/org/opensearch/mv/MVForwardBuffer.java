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
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * POC(mv) forward buffer: VSR with the definition's captured columns (spec-
 * driven — serves both the SOURCE and the TARGET-fold definitions). Rotation
 * exports via Arrow C Data and feeds the native DataFusion-maintained state.
 *
 * <p>Rollback contract unchanged: rotation happens BEFORE appending, so a
 * failed doc is always still in this buffer; truncateTo undoes it.
 */
final class MVForwardBuffer implements AutoCloseable {

    static final int ROTATION_THRESHOLD = 4096;

    private final BufferAllocator allocator;
    private final VectorSchemaRoot vsr;
    private final List<FieldVector> vectors;
    private final List<MVDefinitionSpec.Column> columns;
    private int rowCount = 0;

    MVForwardBuffer(MVDefinitionSpec spec) {
        this.allocator = new RootAllocator(64L * 1024 * 1024);
        this.columns = spec.columns();
        this.vectors = new ArrayList<>(columns.size());
        for (MVDefinitionSpec.Column col : columns) {
            vectors.add(
                col.type() == MVDefinitionSpec.ColumnType.UTF8
                    ? new VarCharVector(col.name(), allocator)
                    : new BigIntVector(col.name(), allocator)
            );
        }
        this.vsr = new VectorSchemaRoot(vectors);
    }

    void append(Object[] row) {
        for (int i = 0; i < columns.size(); i++) {
            FieldVector v = vectors.get(i);
            Object value = row[i];
            if (v instanceof VarCharVector vc) {
                if (value == null) {
                    vc.setNull(rowCount);
                } else {
                    vc.setSafe(rowCount, ((String) value).getBytes(StandardCharsets.UTF_8));
                }
            } else {
                BigIntVector bv = (BigIntVector) v;
                if (value == null) {
                    bv.setNull(rowCount);
                } else {
                    bv.setSafe(rowCount, (Long) value);
                }
            }
        }
        rowCount++;
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
