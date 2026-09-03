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
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVNativeBridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * DataFusion strategy for merging partial MV aggregation states.
 *
 * <p>The generic data-format merge framework selects candidates and schedules
 * the merge. This strategy only defines the format operation:
 * {@code STATE + STATE -> STATE}.
 *
 * <p><b>Stage 4:</b> Merges always use the {@code merge_state_streams} FFI
 * which carries the full ordering contract and accumulator metadata across
 * the boundary. The Rust side validates that all inputs share the expected
 * ordering and applies the correct per-column fold function (SUM-fold for
 * SUM/COUNT, MIN-fold for MIN, MAX-fold for MAX). The legacy SQL-based
 * {@code df_mv_merge_state} path has been removed — all merge callers now
 * go through the streaming engine with typed metadata.
 */
public final class DataFusionMVStateMergeStrategy implements MVMergeStrategy {

    private static final Logger logger = LogManager.getLogger(DataFusionMVStateMergeStrategy.class);

    private final DataFormat dataFormat;
    private final ShardPath shardPath;
    /**
     * @deprecated Retained only for API compatibility with callers that still
     *             pass fold SQL. The streaming merge engine ignores this field
     *             entirely — fold semantics are derived from the compiled
     *             definition's accumulator metadata.
     */
    @Deprecated(forRemoval = true)
    private final String foldSql;
    private final MVCompiledDefinition compiledDefinition;
    private final StreamingMerger streamingMerger;

    /**
     * Stage 4: Primary constructor with compiled definition for streaming merge.
     *
     * @param dataFormat         the MV data format
     * @param shardPath          shard data path
     * @param foldSql            fold SQL template (retained for API compatibility;
     *                           ignored by the streaming merge engine)
     * @param compiledDefinition compiled MV definition (carries ordering +
     *                           aggregate metadata); must not be null
     * @param runtimePtr         retained for API compatibility; no longer
     *                           consulted by the merge gate
     * @throws IllegalArgumentException if compiledDefinition is null
     */
    public DataFusionMVStateMergeStrategy(
        DataFormat dataFormat,
        ShardPath shardPath,
        String foldSql,
        MVCompiledDefinition compiledDefinition,
        long runtimePtr
    ) {
        this(dataFormat, shardPath, foldSql, compiledDefinition, MVNativeBridge::mergeStateStreams);
    }

    /**
     * Test-injectable constructor for unit tests that want to capture the FFI
     * parameters without invoking the native library.
     */
    DataFusionMVStateMergeStrategy(
        DataFormat dataFormat,
        ShardPath shardPath,
        String foldSql,
        MVCompiledDefinition compiledDefinition,
        StreamingMerger streamingMerger
    ) {
        if (compiledDefinition == null) {
            throw new IllegalArgumentException(
                "Stage 4: compiledDefinition is required — the legacy SQL merge path has been removed. "
                    + "Ensure MVCompiledDefinition.compiledFor(definitionName) succeeds before constructing the merge strategy."
            );
        }
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.foldSql = foldSql;
        this.compiledDefinition = compiledDefinition;
        this.streamingMerger = streamingMerger;
    }

    /**
     * Legacy constructor without compiled definition.
     *
     * @deprecated Stage 4: The legacy SQL merge path has been removed. Use the
     *             constructor that accepts a {@link MVCompiledDefinition}.
     *             This constructor now throws {@link IllegalArgumentException}.
     */
    @Deprecated(forRemoval = true)
    public DataFusionMVStateMergeStrategy(DataFormat dataFormat, ShardPath shardPath, String foldSql) {
        throw new IllegalArgumentException(
            "Stage 4: The legacy SQL merge path has been removed. Use the constructor "
                + "that accepts an MVCompiledDefinition for streaming merge with full metadata."
        );
    }

    /**
     * @deprecated Stage 4: Use the test-injectable constructor that accepts
     *             a {@link StreamingMerger} and {@link MVCompiledDefinition}.
     */
    @Deprecated(forRemoval = true)
    DataFusionMVStateMergeStrategy(DataFormat dataFormat, ShardPath shardPath, String foldSql, StateFileMerger stateFileMerger) {
        throw new IllegalArgumentException(
            "Stage 4: The legacy StateFileMerger interface has been replaced by StreamingMerger. "
                + "Use the constructor that accepts MVCompiledDefinition and StreamingMerger."
        );
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
            long rows = mergeWithStreaming(inputPaths, outputFile.toString());

            MonoFileWriterSet mergedFiles = MonoFileWriterSet.of(
                outputDirectory,
                writerGeneration,
                outputFile.getFileName().toString(),
                Math.max(rows, 1L)
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

    /**
     * Stage 4: Merge through the streaming FFI with ordering and accumulator
     * metadata. Uses the compiled definition's
     * {@link MVCompiledDefinition.MergeCallParams} to resolve all FFI
     * parameters in one call. Derives the ordering identity from the PHYSICAL
     * column names in the first input file — the ground truth for what the
     * Rust merge engine will compute. Falls back to the logical alias-based
     * identity if the file schema cannot be read (e.g. in unit tests with
     * mock/empty state files).
     */
    private long mergeWithStreaming(List<String> inputPaths, String outputPath) {
        MVCompiledDefinition.MergeCallParams params;
        try {
            // Use physical ordering identity derived from the first input
            // state file's Arrow IPC schema. Expression group keys have
            // physical names (DataFusion's Partial aggregate Display form)
            // that differ from the SQL alias — reading from the file is
            // authoritative.
            params = compiledDefinition.buildMergeCallParams(inputPaths.get(0));
        } catch (java.io.IOException e) {
            // Fallback: use logical alias-based identity. This happens when
            // the first input file is not a valid Arrow IPC file (e.g. in
            // unit tests with empty placeholder files). In production, the
            // files are always valid IPC and this branch is unreachable.
            logger.warn(
                "mv_merge: could not read Arrow schema from [{}], falling back to logical ordering identity: {}",
                inputPaths.get(0),
                e.getMessage()
            );
            params = compiledDefinition.buildMergeCallParams();
        }

        logger.debug(
            "mv_merge streaming: {} inputs, {} ordering keys, {} total columns, identity=[{}]",
            inputPaths.size(),
            params.orderingIndices().length,
            params.foldOps().length,
            params.orderingIdentity()
        );

        return streamingMerger.merge(inputPaths, outputPath, params);
    }

    /** Compiled definition accessor for test validation. */
    MVCompiledDefinition compiledDefinition() {
        return compiledDefinition;
    }

    /**
     * Stage 4: Functional interface for the streaming merge engine. Replaces
     * the legacy {@link StateFileMerger} with a typed interface that carries
     * the full {@link MVCompiledDefinition.MergeCallParams}.
     */
    @FunctionalInterface
    interface StreamingMerger {
        long merge(List<String> stateFiles, String outputFile, MVCompiledDefinition.MergeCallParams params);
    }

    /**
     * @deprecated Stage 4: Replaced by {@link StreamingMerger}. This interface
     *             carried fold SQL for the legacy {@code df_mv_merge_state}
     *             path. All merge callers now use the streaming engine with
     *             typed metadata from {@link MVCompiledDefinition.MergeCallParams}.
     */
    @Deprecated(forRemoval = true)
    @FunctionalInterface
    interface StateFileMerger {
        long merge(List<String> stateFiles, String foldSql, String outputFile) throws Exception;
    }

    /** Bridge from the new StreamingMerger to the underlying FFI call. */
    private static long mergeStateStreams(List<String> stateFiles, String outputFile, MVCompiledDefinition.MergeCallParams params) {
        return MVNativeBridge.mergeStateStreams(stateFiles, outputFile, params);
    }
}
