/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.mv.MVNativeBridge;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The PRODUCT read path: delta aggregation through the DataFusion engine
 * over the source's PARQUET files (which carry the {@code _seq_no} column).
 * Mirrors the push model's catch-up fold exactly: stage the parquet file
 * set, inject the seq-range predicate into the definition SQL, and execute
 * {@code MVNativeBridge.buildArrow} (native DataFusion Partial fold →
 * Arrow C-Data, zero copy).
 *
 * <p>POC definition (matching the Lucene backend for A/B):
 * {@code SELECT group, COUNT(*), SUM(sum) FROM mv_input GROUP BY group}.
 * COUNT/SUM partial states are their own finals, so the imported batch's
 * rows are directly the delta group states.
 */
final class MVDataFusionReadEngine implements Closeable {

    /**
     * Folded delta plus the max {@code _seq_no} actually present in the pulled data for the range,
     * and the total number of source rows that contributed (sum of per-group COUNTs).
     */
    record Delta(Map<Long, long[]> groups, long observedMaxSeqNo, long totalRows) {
    }

    private static final Logger logger = LogManager.getLogger(MVDataFusionReadEngine.class);
    private static final String INPUT_TABLE = "mv_input";

    private final Path stagingRoot;

    MVDataFusionReadEngine(Path workingPath) throws IOException {
        this.stagingRoot = workingPath.resolve("df_stage");
        Files.createDirectories(stagingRoot);
    }

    /**
     * Runs the delta SEARCH through DataFusion over the advert's parquet
     * files: {@code WHERE _seq_no > fromExclusive AND _seq_no <= toInclusive}
     * wrapped around the definition, exactly the catch-up
     * {@code filteredSql} shape.
     */
    Delta searchDelta(List<Path> parquetFiles, String groupField, String sumField, long fromExclusive, long toInclusive, long infosVersion)
        throws IOException {
        // Stage the generation's parquet set under one directory (DataFusion
        // lists a directory table). Symlinks keep this zero-copy on disk.
        Path staged = stagingRoot.resolve("gen-" + infosVersion);
        if (Files.exists(staged) == false) {
            Files.createDirectories(staged);
            int i = 0;
            for (Path file : parquetFiles) {
                Files.createSymbolicLink(staged.resolve(String.format(Locale.ROOT, "%06d.parquet", i++)), file.toAbsolutePath());
            }
        }
        // MAX("_seq_no") per group lets the caller advance the watermark by
        // what the pulled data ACTUALLY contains: the advertised checkpoint can
        // over-claim relative to the uploaded catalog content (composite upload
        // race); over-claimed ops arrive in a later generation and self-heal.
        String sql = String.format(
            Locale.ROOT,
            "SELECT \"%s\", COUNT(*), SUM(\"%s\"), MAX(\"_seq_no\") FROM (SELECT * FROM %s WHERE \"_seq_no\" > %d AND \"_seq_no\" <= %d) AS %s GROUP BY \"%s\"",
            groupField,
            sumField,
            INPUT_TABLE,
            fromExclusive,
            toInclusive,
            INPUT_TABLE,
            groupField
        );
        Map<Long, long[]> groups = new HashMap<>();
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            try (
                org.apache.arrow.c.ArrowArray array = org.apache.arrow.c.ArrowArray.allocateNew(allocator);
                org.apache.arrow.c.ArrowSchema schema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
            ) {
                long rows;
                try {
                    rows = MVNativeBridge.buildArrow(staged.toString(), INPUT_TABLE, sql, array.memoryAddress(), schema.memoryAddress());
                } catch (RuntimeException noRows) {
                    // Same contract as the catch-up fold: an empty range is a
                    // legitimate no-batches result, anything else is fatal.
                    if (noRows.getMessage() != null && noRows.getMessage().contains("partial produced no batches")) {
                        cleanupStaged(staged);
                        return new Delta(groups, -1L, 0L);
                    }
                    throw noRows;
                }
                try (
                    org.apache.arrow.vector.VectorSchemaRoot batch = org.apache.arrow.c.Data.importVectorSchemaRoot(
                        allocator,
                        array,
                        schema,
                        null
                    )
                ) {
                    List<org.apache.arrow.vector.FieldVector> vectors = batch.getFieldVectors();
                    long observedMax = -1L;
                    long totalRowCount = 0L;
                    for (int row = 0; row < batch.getRowCount(); row++) {
                        long group = ((Number) vectors.get(0).getObject(row)).longValue();
                        long cnt = ((Number) vectors.get(1).getObject(row)).longValue();
                        Object sumValue = vectors.get(2).getObject(row);
                        long sum = sumValue == null ? 0L : ((Number) sumValue).longValue();
                        Object maxSeq = vectors.get(3).getObject(row);
                        if (maxSeq != null) {
                            observedMax = Math.max(observedMax, ((Number) maxSeq).longValue());
                        }
                        groups.put(group, new long[] { cnt, sum });
                        totalRowCount += cnt;
                    }
                    logger.debug(
                        "mv_pull datafusion delta rows={} range=({}, {}] observedMax={} totalSourceRows={}",
                        rows,
                        fromExclusive,
                        toInclusive,
                        observedMax,
                        totalRowCount
                    );
                    return new Delta(groups, observedMax, totalRowCount);
                }
            }
        } finally {
            cleanupStaged(staged);
        }
    }

    private void cleanupStaged(Path staged) {
        try {
            if (Files.exists(staged)) {
                try (var stream = Files.list(staged)) {
                    for (Path p : stream.toList()) {
                        Files.deleteIfExists(p);
                    }
                }
                Files.deleteIfExists(staged);
            }
        } catch (IOException e) {
            logger.warn("mv_pull failed to clean staged fold dir " + staged, e);
        }
    }

    Path stageParquetFiles(List<Path> parquetFiles, long generation) throws IOException {
        Path staged = stagingRoot.resolve("artifact-" + generation);
        Files.createDirectories(staged);
        int i = 0;
        for (Path file : parquetFiles) {
            Path link = staged.resolve(String.format(Locale.ROOT, "%06d.parquet", i++));
            if (Files.exists(link) == false) {
                Files.createSymbolicLink(link, file.toAbsolutePath());
            }
        }
        return staged;
    }

    void cleanupStagedParquet(Path staged) {
        cleanupStaged(staged);
    }

    @Override
    public void close() throws IOException {
        // staging dirs are per-round and cleaned per-round
    }
}
