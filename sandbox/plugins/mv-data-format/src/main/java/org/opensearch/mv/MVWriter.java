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
 * POC(mv) writer — REFRESH-TIME BUILD model (decision 18): the complete MV
 * state for a generation is computed once, at flush/refresh, by running the
 * definition's Partial stage over the generation's just-flushed PRIMARY
 * parquet file. Nothing MV-related happens on the doc write path.
 *
 * <p>Consequences (why this is the chosen trade):
 * <ul>
 *   <li>addDoc is a row count — zero per-doc capture cost, no forward
 *       buffer, no native writer state held across the generation;</li>
 *   <li>rollback is trivially safe (nothing exists to unwind before
 *       flush) — the rotate-before-append contract is gone with the
 *       machinery that needed it;</li>
 *   <li>the flush does one extra read of the parquet it just wrote (page
 *       cache hot) plus the aggregation — accepted cost, paid once per
 *       refresh, off the ingest hot path;</li>
 *   <li>path coupling to the primary's file naming — same convention the
 *       merger's recompute already relies on.</li>
 * </ul>
 *
 * <p>The streaming/VSR incremental-fold model this replaces remains in git
 * history (and native {@code mv_writer.rs}) as the optimization path if
 * refresh-time build cost ever matters.
 */
public final class MVWriter implements Writer<MVDocumentInput> {

    private static final Logger logger = LogManager.getLogger(MVWriter.class);

    private final long writerGeneration;
    private final ShardPath shardPath;
    private final String tableName;
    private final MVDefinitionSpec spec;
    /** Non-null = separate-index mode: ship state rows before commit, keep no local file. */
    private final MVStateShipper shipper;
    private long acceptedRows = 0;
    private volatile long mappingVersion = 1L;
    private volatile WriterState state = WriterState.ACTIVE;

    /** The format this writer registers its files under (source vs target-fold). */
    private final org.opensearch.index.engine.dataformat.DataFormat dataFormat;

    public MVWriter(long writerGeneration, ShardPath shardPath, String tableName) {
        this(writerGeneration, shardPath, tableName, MVDefinitionSpec.SOURCE, MVDataFormat.INSTANCE, null);
    }

    public MVWriter(
        long writerGeneration,
        ShardPath shardPath,
        String tableName,
        MVDefinitionSpec spec,
        org.opensearch.index.engine.dataformat.DataFormat dataFormat,
        MVStateShipper shipper
    ) {
        this.writerGeneration = writerGeneration;
        this.shardPath = shardPath;
        this.tableName = tableName;
        this.spec = spec;
        this.dataFormat = dataFormat;
        this.shipper = shipper;
    }

    @Override
    public WriteResult addDoc(MVDocumentInput doc) {
        // Refresh-time build: the write path only counts rows (the count
        // drives the "emit nothing for an empty generation" leg of flush).
        acceptedRows++;
        return new WriteResult.Success(1L, 1L, 1L);
    }

    @Override
    public void rollbackTo(long rowCount) {
        // Nothing accumulates before flush — rollback is a counter reset.
        acceptedRows = rowCount;
        state = WriterState.ACTIVE;
    }

    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        if (acceptedRows == 0) {
            // Nothing ingested this generation — emit nothing ("or none" leg).
            return FileInfos.empty();
        }

        // The composite flush runs the PRIMARY first, so this generation's
        // parquet file exists (and is page-cache hot) by the time we run.
        Path parquet = flushedPrimaryFile();

        if (shipper != null) {
            // Separate-index mode (Approach 2): build the state batch from
            // the flushed parquet and hand it to the target as live Arrow
            // buffers via C-Data. Ship-before-commit: only a durable +
            // searchable ack lets the flush succeed. The source tracks NO
            // MV files (the MV index owns its layout).
            try (org.apache.arrow.memory.RootAllocator shipAllocator = new org.apache.arrow.memory.RootAllocator()) {
                try (
                    org.apache.arrow.c.ArrowArray array = org.apache.arrow.c.ArrowArray.allocateNew(shipAllocator);
                    org.apache.arrow.c.ArrowSchema schema = org.apache.arrow.c.ArrowSchema.allocateNew(shipAllocator)
                ) {
                    long stateRows = MVNativeBridge.buildArrow(
                        parquet.toString(),
                        MVConstants.INPUT_TABLE,
                        spec.sql(),
                        array.memoryAddress(),
                        schema.memoryAddress()
                    );
                    org.apache.arrow.vector.VectorSchemaRoot stateBatch = org.apache.arrow.c.Data.importVectorSchemaRoot(
                        shipAllocator,
                        array,
                        schema,
                        null
                    );
                    // Ownership passes to the ship action's handler (closes in
                    // its try/finally); on ship failure the flush fails either way.
                    long shipped = shipper.ship(stateBatch, writerGeneration);
                    logger.info("mv flush gen={} shipped {} of {} state rows before commit", writerGeneration, shipped, stateRows);
                }
            }
            return FileInfos.empty();
        }

        // Embedded mode: build the state file (Arrow IPC) next to the shard.
        Path mvDir = shardPath.getDataPath().resolve(dataFormat.name());
        Files.createDirectories(mvDir);
        Path mvFile = mvDir.resolve(MVConstants.mvFileName(writerGeneration));

        long stateRows = MVNativeBridge.buildStateFile(parquet.toString(), MVConstants.INPUT_TABLE, spec.sql(), mvFile.toString());
        logger.info("mv flush gen={} built {} state rows at refresh -> {}", writerGeneration, stateRows, mvFile.getFileName());

        MonoFileWriterSet fileSet = MonoFileWriterSet.of(
            mvDir.toAbsolutePath(),
            writerGeneration,
            mvFile.getFileName().toString(),
            Math.max(stateRows, 1)
        );
        return FileInfos.builder().putWriterFileSet(dataFormat, fileSet).build();
    }

    /** This generation's primary parquet file, by the engine's naming convention. */
    private Path flushedPrimaryFile() throws IOException {
        Path parquetDir = shardPath.getDataPath().resolve("parquet");
        Path flushed = parquetDir.resolve("_parquet_file_generation_" + Long.toHexString(writerGeneration) + ".parquet");
        if (Files.exists(flushed) == false) {
            throw new IOException("mv flush: primary parquet not found for gen " + writerGeneration + " at " + flushed);
        }
        return flushed;
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
