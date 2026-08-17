/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * POC(mv) writer — VSR/streaming model: MV-referenced column values captured
 * per doc into a forward buffer; each rotation folds the batch into the
 * native sorted background state; flush finalizes the background state as
 * the sorted MV state parquet. No read of the primary's file.
 *
 * <p>Rollback contract: rotation happens BEFORE appending (addDoc), so a
 * failed doc is always still in the forward buffer and rollbackTo only
 * truncates buffered rows — folded state is never unwound.
 */
public final class MVWriter implements Writer<MVDocumentInput> {

    private static final Logger logger = LogManager.getLogger(MVWriter.class);

    private final long writerGeneration;
    private final ShardPath shardPath;
    private final String tableName;
    private final MVForwardBuffer buffer;
    private final long nativeWriter;
    /** Non-null = separate-index mode: ship state rows before commit, keep no local file. */
    private final MVStateShipper shipper;
    private long acceptedRows = 0;
    private long foldedRows = 0; // rows already rotated into the background state
    private volatile long mappingVersion = 1L;
    private volatile WriterState state = WriterState.ACTIVE;

    public MVWriter(long writerGeneration, ShardPath shardPath, String tableName) {
        this(writerGeneration, shardPath, tableName, null);
    }

    public MVWriter(long writerGeneration, ShardPath shardPath, String tableName, MVStateShipper shipper) {
        this.writerGeneration = writerGeneration;
        this.shardPath = shardPath;
        this.tableName = tableName;
        this.shipper = shipper;
        this.buffer = new MVForwardBuffer();
        this.nativeWriter = MVNativeBridge.writerCreate(MVConstants.MV_SQL, MVConstants.GROUP_KEYS.size());
    }

    @Override
    public WriteResult addDoc(MVDocumentInput doc) {
        // Rotate BEFORE appending so rollback of THIS doc only touches the buffer.
        if (buffer.shouldRotate()) {
            buffer.rotateInto(nativeWriter);
            foldedRows = acceptedRows;
        }
        buffer.append(doc.getFinalInput());
        acceptedRows++;
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void rollbackTo(long rowCount) {
        if (rowCount < foldedRows) {
            // Would require unwinding folded state — cannot happen under the
            // rotate-before-append rule (composite rolls back at most the doc
            // that just failed). Fail loudly if the assumption breaks.
            throw new IllegalStateException("mv rollback below folded watermark: " + rowCount + " < " + foldedRows);
        }
        buffer.truncateTo((int) (rowCount - foldedRows));
        acceptedRows = rowCount;
        state = WriterState.ACTIVE;
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        if (acceptedRows == 0) {
            // Nothing ingested this generation — emit nothing ("or none" leg).
            return FileInfos.empty();
        }

        // Final rotation, then persist the background state (already sorted).
        buffer.rotateInto(nativeWriter);
        foldedRows = acceptedRows;

        Path mvDir = shardPath.getDataPath().resolve(MVConstants.DIR);
        Files.createDirectories(mvDir);

        if (shipper != null) {
            // Separate-index mode (Approach 2): finalize the state batch and
            // hand it to the target as the LIVE ARROW BUFFERS via C-Data —
            // no scratch file, no row re-encoding, zero copies (the handler
            // reads the same memory the native fold produced). Ship-before-
            // commit: only a durable+searchable ack lets the flush succeed.
            // The source tracks NO MV files (the MV index owns its layout).
            org.apache.arrow.memory.BufferAllocator shipAllocator = buffer.allocator();
            try (
                org.apache.arrow.c.ArrowArray array = org.apache.arrow.c.ArrowArray.allocateNew(shipAllocator);
                org.apache.arrow.c.ArrowSchema schema = org.apache.arrow.c.ArrowSchema.allocateNew(shipAllocator)
            ) {
                long stateRows = MVNativeBridge.writerFinalizeArrow(nativeWriter, array.memoryAddress(), schema.memoryAddress());
                org.apache.arrow.vector.VectorSchemaRoot stateBatch = org.apache.arrow.c.Data.importVectorSchemaRoot(
                    shipAllocator,
                    array,
                    schema,
                    null
                );
                // Ownership passes to the ship action's handler (closes in its
                // try/finally); on ship failure the flush fails either way.
                long shipped = shipper.ship(stateBatch, writerGeneration);
                logger.info("mv flush gen={} shipped {} of {} state rows before commit", writerGeneration, shipped, stateRows);
            }
            return FileInfos.empty();
        }

        Path mvFile = mvDir.resolve(MVConstants.mvFileName(writerGeneration));

        long stateRows = MVNativeBridge.writerFinalize(nativeWriter, mvFile.toString());
        logger.info("mv flush gen={} streamed {} state rows -> {}", writerGeneration, stateRows, mvFile.getFileName());

        MonoFileWriterSet fileSet = MonoFileWriterSet.of(
            mvDir.toAbsolutePath(),
            writerGeneration,
            mvFile.getFileName().toString(),
            Math.max(stateRows, 1)
        );
        return FileInfos.builder().putWriterFileSet(MVDataFormat.INSTANCE, fileSet).build();
    }

    @Override
    public long generation() {
        return writerGeneration;
    }

    @Override
    public WriterState state() {
        return state;
    }

    @Override
    public boolean isSchemaMutable() {
        return true;
    }

    @Override
    public long mappingVersion() {
        return mappingVersion;
    }

    @Override
    public void updateMappingVersion(long newVersion) {
        this.mappingVersion = newVersion;
    }

    @Override
    public void close() {
        if (state != WriterState.CLOSED) {
            MVNativeBridge.writerAbort(nativeWriter);
            buffer.close();
            state = WriterState.CLOSED;
        }
    }
}
