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
    private long acceptedRows = 0;
    private long foldedRows = 0; // rows already rotated into the background state
    private volatile long mappingVersion = 1L;
    private volatile WriterState state = WriterState.ACTIVE;

    public MVWriter(long writerGeneration, ShardPath shardPath, String tableName) {
        this.writerGeneration = writerGeneration;
        this.shardPath = shardPath;
        this.tableName = tableName;
        this.buffer = new MVForwardBuffer();
        this.nativeWriter = MVNativeBridge.writerCreate();
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
