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
 * POC(mv) writer: a derived-format writer. Never buffers documents — the MV
 * state file is computed at flush from the primary's flushed parquet file
 * (primary flushes first in CompositeWriter, so the file exists on disk).
 *
 * <p>Contract honored: whenever the primary produced a file for this
 * generation, this writer produces the MV state file (composite "all formats
 * or none" flush assert). Zero-group results still produce a valid file.
 */
public final class MVWriter implements Writer<MVDocumentInput> {

    private static final Logger logger = LogManager.getLogger(MVWriter.class);

    // POC HACK: primary parquet path convention copied from ParquetIndexingEngine
    // (avoids a compile dep on the parquet plugin for one string).
    private static final String PARQUET_DIR = "parquet";
    private static final String PARQUET_PREFIX = "_parquet_file_generation";

    private final long writerGeneration;
    private final ShardPath shardPath;
    private final String tableName;
    private long acceptedRows = 0;
    private volatile long mappingVersion = 1L;
    private volatile WriterState state = WriterState.ACTIVE;

    public MVWriter(long writerGeneration, ShardPath shardPath, String tableName) {
        this.writerGeneration = writerGeneration;
        this.shardPath = shardPath;
        this.tableName = tableName;
    }

    @Override
    public WriteResult addDoc(MVDocumentInput doc) {
        // Derived format: nothing to store per doc; track accepted rows for the
        // composite cross-format protocol.
        acceptedRows++;
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void rollbackTo(long rowCount) {
        // No buffered state to undo.
        acceptedRows = rowCount;
        state = WriterState.ACTIVE;
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        Path primaryFile = primaryParquetPath();
        if (Files.exists(primaryFile) == false) {
            // Primary flushed nothing (empty writer) — emit nothing ("or none").
            logger.debug("mv flush gen={} no primary file at {}, skipping", writerGeneration, primaryFile);
            return FileInfos.empty();
        }

        Path mvDir = shardPath.getDataPath().resolve(MVConstants.DIR);
        Files.createDirectories(mvDir);
        Path mvFile = mvDir.resolve(MVConstants.mvFileName(writerGeneration));

        String sql = String.format(java.util.Locale.ROOT, MVConstants.MV_SQL, tableName);
        long stateRows = MVNativeBridge.buildStateFile(primaryFile.toString(), tableName, sql, mvFile.toString());
        logger.info("mv flush gen={} built {} state rows -> {}", writerGeneration, stateRows, mvFile.getFileName());

        MonoFileWriterSet fileSet = MonoFileWriterSet.of(
            mvDir.toAbsolutePath(),
            writerGeneration,
            mvFile.getFileName().toString(),
            Math.max(stateRows, 1) // guard: numRows must be positive for catalog asserts; empty-groups edge
        );
        return FileInfos.builder().putWriterFileSet(MVDataFormat.INSTANCE, fileSet).build();
    }

    private Path primaryParquetPath() {
        return shardPath.getDataPath()
            .resolve(PARQUET_DIR)
            .resolve(PARQUET_PREFIX + "_" + Long.toHexString(writerGeneration) + ".parquet");
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
        state = WriterState.CLOSED;
    }
}
