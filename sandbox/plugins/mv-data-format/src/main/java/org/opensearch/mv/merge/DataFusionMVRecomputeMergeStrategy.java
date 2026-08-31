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
import org.opensearch.index.shard.ShardPath;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVNativeBridge;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * DataFusion strategy for recomputing an MV secondary-format output from the
 * primary Parquet file produced by the same standard merge operation.
 */
public final class DataFusionMVRecomputeMergeStrategy implements MVMergeStrategy {

    private static final Logger logger = LogManager.getLogger(DataFusionMVRecomputeMergeStrategy.class);

    private final DataFormat dataFormat;
    private final ShardPath shardPath;
    private final String definitionSql;

    public DataFusionMVRecomputeMergeStrategy(DataFormat dataFormat, ShardPath shardPath, String definitionSql) {
        this.dataFormat = dataFormat;
        this.shardPath = shardPath;
        this.definitionSql = definitionSql;
    }

    @Override
    public MergeResult mergeMVFiles(MergeInput mergeInput) throws IOException {
        long writerGeneration = mergeInput.newWriterGeneration();
        assert writerGeneration > 0 : "merge writer generation must be positive but was: " + writerGeneration;

        Path parquetDirectory = shardPath.getDataPath().resolve("parquet");
        Path mergedParquet = parquetDirectory.resolve("_parquet_file_generation_merged_" + Long.toHexString(writerGeneration) + ".parquet");
        if (Files.exists(mergedParquet) == false) {
            mergedParquet = parquetDirectory.resolve("_parquet_file_generation_" + Long.toHexString(writerGeneration) + ".parquet");
        }
        if (Files.exists(mergedParquet) == false) {
            throw new IOException("MV merge: merged Parquet file not found for generation " + writerGeneration + " in " + parquetDirectory);
        }

        Path outputDirectory = shardPath.getDataPath().resolve(dataFormat.name());
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(MVConstants.mvFileName(writerGeneration));

        try {
            long rows = MVNativeBridge.buildStateFile(
                mergedParquet.toString(),
                MVConstants.INPUT_TABLE,
                definitionSql,
                outputFile.toString()
            );
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
            logger.error("DataFusion MV recompute merge failed for output [{}]", outputFile, e);
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("DataFusion MV recompute merge failed", e);
        }
    }
}
