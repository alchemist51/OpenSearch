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
 */
public final class DataFusionMVStateMergeStrategy implements MVMergeStrategy {

    private static final Logger logger = LogManager.getLogger(DataFusionMVStateMergeStrategy.class);

    private final DataFormat dataFormat;
    private final ShardPath shardPath;
    private final String foldSql;
    private final StateFileMerger stateFileMerger;

    public DataFusionMVStateMergeStrategy(DataFormat dataFormat, ShardPath shardPath, String foldSql) {
        this(dataFormat, shardPath, foldSql, MVNativeBridge::mergeStateFiles);
    }

    DataFusionMVStateMergeStrategy(DataFormat dataFormat, ShardPath shardPath, String foldSql, StateFileMerger stateFileMerger) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.foldSql = foldSql;
        this.stateFileMerger = stateFileMerger;
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
            long rows = stateFileMerger.merge(stateFiles.stream().map(Path::toString).toList(), foldSql, outputFile.toString());
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

    @FunctionalInterface
    interface StateFileMerger {
        long merge(List<String> stateFiles, String foldSql, String outputFile) throws Exception;
    }
}
