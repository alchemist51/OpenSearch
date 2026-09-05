/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVNativeBridge;
import org.opensearch.mv.MVStateSchemaReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Generic DataFusion merge strategy for MV state generations:
 * N individually sorted parquet files in, ONE sorted parquet file out.
 *
 * <p>The data-format merge framework (TieredMergePolicy selection +
 * MergeScheduler) decides WHAT and WHEN to merge; this strategy only defines
 * the format operation. Rows are merged in the files' declared sort order and
 * written verbatim — no folding, no definition semantics (the Lucene merge
 * idiom: consolidate files, never combine rows). Query-time fold already
 * re-aggregates duplicate keys across state rows, so merging changes file
 * layout, not results.
 *
 * <p>Sort columns are the leading {@code sortKeyCount} PHYSICAL column names
 * of the first input file (read from its parquet footer — the ground truth
 * for expression keys whose physical names differ from SQL aliases), with the
 * ordering contract ASC + NULLS FIRST. Executes on the shared
 * DataFusionRuntime (pool-tracked, breaker-covered, streaming k-way merge).
 */
public final class DataFusionMVStateMergeStrategy implements MVMergeStrategy {

    private static final Logger logger = LogManager.getLogger(DataFusionMVStateMergeStrategy.class);

    private final DataFormat dataFormat;
    private final ShardPath shardPath;
    /** Leading sort-key column count (plain config from the target's ordering contract). */
    private final int sortKeyCount;
    /** Shared DataFusionRuntime pointer; must be non-zero. */
    private final long runtimePtr;
    private final SortedMerger merger;
    private final SortColumnReader sortColumnReader;

    public DataFusionMVStateMergeStrategy(DataFormat dataFormat, ShardPath shardPath, int sortKeyCount, long runtimePtr) {
        this(dataFormat, shardPath, sortKeyCount, runtimePtr, MVNativeBridge::sortedParquetMerge, MVStateSchemaReader::readGroupKeyNames);
    }

    /** Test-injectable constructor capturing FFI parameters without native calls. */
    DataFusionMVStateMergeStrategy(
        DataFormat dataFormat,
        ShardPath shardPath,
        int sortKeyCount,
        long runtimePtr,
        SortedMerger merger,
        SortColumnReader sortColumnReader
    ) {
        if (sortKeyCount <= 0) {
            throw new IllegalArgumentException("sortKeyCount must be positive but was: " + sortKeyCount);
        }
        if (runtimePtr == 0) {
            throw new IllegalArgumentException("runtimePtr must be a valid shared DataFusionRuntime pointer");
        }
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.sortKeyCount = sortKeyCount;
        this.runtimePtr = runtimePtr;
        this.merger = merger;
        this.sortColumnReader = sortColumnReader;
    }

    @Override
    public MergeResult mergeMVFiles(MergeInput mergeInput) throws IOException {
        List<WriterFileSet> fileSets = mergeInput.getFilesForFormat(dataFormat.name());
        if (fileSets.isEmpty()) {
            throw new IllegalArgumentException("No " + dataFormat.name() + " files to merge");
        }

        List<Path> stateFiles = new ArrayList<>();
        for (WriterFileSet fileSet : fileSets) {
            for (String file : fileSet.files()) {
                stateFiles.add(Path.of(fileSet.directory(), file));
            }
        }
        stateFiles.sort(Comparator.comparing(Path::toString));
        if (stateFiles.isEmpty()) {
            throw new IllegalArgumentException("No " + dataFormat.name() + " files to merge");
        }
        for (Path stateFile : stateFiles) {
            if (Files.exists(stateFile) == false) {
                throw new IOException("MV state merge input does not exist: " + stateFile);
            }
        }

        long writerGeneration = mergeInput.newWriterGeneration();
        assert writerGeneration > 0 : "merge writer generation must be positive but was: " + writerGeneration;

        Path outputDirectory = shardPath.getDataPath().resolve(dataFormat.name());
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(MVConstants.mvFileName(writerGeneration));

        try {
            List<String> inputPaths = stateFiles.stream().map(Path::toString).toList();

            // Physical sort column names from the first input's parquet footer —
            // fail closed if unreadable (never merge on guessed ordering).
            List<String> keyNames = sortColumnReader.read(inputPaths.get(0), sortKeyCount);
            String[] sortColumns = keyNames.toArray(new String[0]);
            // Ordering contract: full-key ASC + NULLS FIRST.
            boolean[] descending = new boolean[sortColumns.length];
            boolean[] nullsFirst = new boolean[sortColumns.length];
            java.util.Arrays.fill(nullsFirst, true);

            logger.debug(
                "mv_merge sorted: {} inputs -> gen {} on shared runtime, sort={}",
                inputPaths.size(),
                writerGeneration,
                keyNames
            );

            long rows = merger.merge(runtimePtr, inputPaths, sortColumns, descending, nullsFirst, outputFile.toString());

            // Seq-range propagation (metadata, not data semantics): merged range
            // is min/max across inputs when ALL are known; any UNKNOWN input
            // propagates UNKNOWN so the checkpoint handler never overclaims.
            long mergedMinSeq = WriterFileSet.UNKNOWN_SEQ_NO;
            long mergedMaxSeq = WriterFileSet.UNKNOWN_SEQ_NO;
            boolean allRangesKnown = fileSets.stream()
                .allMatch(f -> f.minSeqNo() != WriterFileSet.UNKNOWN_SEQ_NO && f.maxSeqNo() != WriterFileSet.UNKNOWN_SEQ_NO);
            if (allRangesKnown) {
                mergedMinSeq = fileSets.stream().mapToLong(WriterFileSet::minSeqNo).min().orElse(WriterFileSet.UNKNOWN_SEQ_NO);
                mergedMaxSeq = fileSets.stream().mapToLong(WriterFileSet::maxSeqNo).max().orElse(WriterFileSet.UNKNOWN_SEQ_NO);
            }

            MonoFileWriterSet mergedFiles = MonoFileWriterSet.of(
                outputDirectory,
                writerGeneration,
                outputFile.getFileName().toString(),
                Math.max(rows, 1L),
                0L,
                mergedMinSeq,
                mergedMaxSeq
            );
            return new MergeResult(Map.of(dataFormat, mergedFiles));
        } catch (Exception e) {
            try {
                Files.deleteIfExists(outputFile);
            } catch (IOException cleanupException) {
                e.addSuppressed(cleanupException);
            }
            logger.error("DataFusion MV state merge failed for output [{}]", outputFile, e);
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("DataFusion MV state merge failed", e);
        }
    }

    /** Reads the leading sort-column names from a state file's parquet footer. */
    @FunctionalInterface
    interface SortColumnReader {
        List<String> read(String stateFile, int count) throws IOException;
    }

    /** Functional interface mirroring {@link MVNativeBridge#sortedParquetMerge}. */
    @FunctionalInterface
    interface SortedMerger {
        long merge(
            long runtimePtr,
            List<String> inputFiles,
            String[] sortColumns,
            boolean[] descending,
            boolean[] nullsFirst,
            String outputFile
        );
    }
}
