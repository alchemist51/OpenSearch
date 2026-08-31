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
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVStateArtifactWriter;
import org.opensearch.mv.MVStateDataFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * MV-specific implementation of {@link DerivedArtifactBuilder}. Runs the
 * DataFusion fold over staged parquet files, validates coverage, writes the
 * Arrow state artifact, and publishes it via {@link IndexShard}.
 *
 * <p>This class encapsulates all MV/DataFusion/Arrow/coverage logic that
 * was previously inline in the old {@code MVArtifactPoller}. The generic
 * poller never sees any of these types.</p>
 */
final class MVDerivedArtifactBuilder implements DerivedArtifactBuilder {

    private static final Logger logger = LogManager.getLogger(MVDerivedArtifactBuilder.class);

    private final IndexSettings indexSettings;
    private final MVPullSettings.Services services;
    private final MVCompiledDefinition compiledDefinition;
    private final MVStateArtifactWriter artifactWriter = new MVStateArtifactWriter();

    private volatile MVDataFusionReadEngine coverageReader;
    private volatile MVWatermark watermark;

    MVDerivedArtifactBuilder(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.services = services;

        // Build compiled definition from settings
        Settings settings = indexSettings.getSettings();
        String definitionName = settings.get(MVConstants.DEFINITION_SETTING, "payments");

        // Build from MVDefinitionSpec (named definitions only).
        // Legacy group_field/sum_field fallback is removed — all pull builds
        // must use a named MVDefinitionSpec with MVCompiledDefinition.buildPartialSql().
        var spec = org.opensearch.mv.MVDefinitionSpec.source(definitionName);
        this.compiledDefinition = buildFromSpec(spec);

        // Validate definition hash if persisted
        String persistedHash = MVPullSettings.DEFINITION_HASH.get(settings);
        if (persistedHash != null && persistedHash.isEmpty() == false) {
            if (persistedHash.equals(compiledDefinition.hash()) == false) {
                throw new IllegalStateException(
                    "mv_pull: definition hash mismatch: persisted=["
                        + persistedHash
                        + "] computed=["
                        + compiledDefinition.hash()
                        + "]. The MV definition has changed since the index was created."
                );
            }
        }
    }

    @Override
    public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
        MVDerivedSourceReader.MVSourceSnapshot mvSnapshot = (MVDerivedSourceReader.MVSourceSnapshot) snapshot;

        // Initialize coverageReader lazily
        if (coverageReader == null) {
            coverageReader = new MVDataFusionReadEngine(shard.shardPath().getDataPath().resolve("mv_pull_work"));
        }

        // Recover watermark on first build
        if (watermark == null) {
            DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
            int sourceShardId = binding != null ? binding.resolveSourceShard(shard.shardId().id()) : 0;
            watermark = recoveredWatermark(shard, sourceShardId);
        }

        MVWatermark current = watermark;

        // Per-round binding validation
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        if (binding != null) {
            DerivedIndexBinding.ValidationResult result = binding.validateLive(
                services.clusterService().state().metadata().index(binding.sourceName())
            );
            if (result.isValid() == false) {
                logger.error("mv_pull binding validation failed for shard [{}]: {}", shard.shardId(), result.reason());
                return new MVBuildResult(false, "binding-validation-failed", Map.of());
            }
        }

        // List parquet files in stage dir
        List<Path> parquetFiles;
        try (Stream<Path> files = Files.list(stageDir)) {
            parquetFiles = files.filter(p -> p.toString().endsWith(".parquet")).sorted().toList();
        }
        if (parquetFiles.isEmpty()) {
            return new MVBuildResult(false, "no-parquet-files", Map.of());
        }

        // Use compiled definition to build SQL
        String baseSql = compiledDefinition.buildPartialSql(MVConstants.INPUT_TABLE);
        String filteredSql = wrapWithSeqNoFilter(baseSql, current.seqNo(), mvSnapshot.watermark());

        // Run coverage check via DataFusion
        String groupField = compiledDefinition.groupKeys().get(0).name();
        String sumField = compiledDefinition.aggregates().size() > 1
            ? compiledDefinition.aggregates().get(1).sourceField()
            : compiledDefinition.aggregates().get(0).sourceField();
        MVDataFusionReadEngine.Delta coverage = coverageReader.searchDelta(
            parquetFiles,
            groupField,
            sumField != null ? sumField : groupField,
            current.seqNo(),
            mvSnapshot.watermark(),
            mvSnapshot.infosVersion()
        );

        if (coverage.observedMaxSeqNo() < 0L) {
            return new MVBuildResult(false, "no-coverage", Map.of());
        }
        long appliedThrough = Math.min(coverage.observedMaxSeqNo(), mvSnapshot.watermark());

        // Coverage integrity guard
        long expectedRows = appliedThrough - current.seqNo();
        if (MVWatermark.hasCompleteCoverage(current.seqNo(), appliedThrough, coverage.totalRows()) == false) {
            logger.warn(
                "mv_pull coverage mismatch: range=({}, {}] expected={} but found={}",
                current.seqNo(),
                appliedThrough,
                expectedRows,
                coverage.totalRows()
            );
            return new MVBuildResult(false, "coverage-mismatch", Map.of());
        }

        // Build artifact
        long generation = shard.reserveDerivedArtifactGeneration();
        Path stagedParquet = coverageReader.stageParquetFiles(parquetFiles, generation);
        try {
            MVStateArtifactWriter.Artifact artifact = artifactWriter.build(
                stagedParquet,
                MVConstants.INPUT_TABLE,
                filteredSql,
                shard.shardPath().getDataPath(),
                generation
            );

            MVWatermark next = new MVWatermark(mvSnapshot.primaryTerm(), appliedThrough, mvSnapshot.infosVersion());

            // Publish
            shard.publishDerivedArtifact(
                MVStateDataFormat.INSTANCE,
                artifact.fileSet(),
                Map.of(MVWatermark.key(shard.shardId().id()), next.encode())
            );
            watermark = next;

            logger.info(
                "mv_pull published generation={} rows={} range=({}, {}] watermark={}",
                generation,
                artifact.stateRows(),
                current.seqNo(),
                appliedThrough,
                next
            );

            return new MVBuildResult(true, "gen-" + generation, Map.of("stateRows", artifact.stateRows(), "generation", generation));
        } finally {
            coverageReader.cleanupStagedParquet(stagedParquet);
        }
    }

    @Override
    public void close() throws IOException {
        if (coverageReader != null) {
            coverageReader.close();
        }
    }

    private static String wrapWithSeqNoFilter(String baseSql, long fromExclusive, long toInclusive) {
        // Inject the seq-no range filter into the FROM clause only — the aggregated
        // output does not carry _seq_no so an outer WHERE would fail.
        return baseSql.replace(
            "FROM " + MVConstants.INPUT_TABLE,
            "FROM (SELECT * FROM "
                + MVConstants.INPUT_TABLE
                + " WHERE \"_seq_no\" > "
                + fromExclusive
                + " AND \"_seq_no\" <= "
                + toInclusive
                + ") AS "
                + MVConstants.INPUT_TABLE
        );
    }

    private static MVWatermark recoveredWatermark(IndexShard shard, int sourceShardId) throws IOException {
        try (var ref = shard.getCatalogSnapshot()) {
            String encoded = ref.get().getUserData().get(MVWatermark.key(sourceShardId));
            return encoded == null ? MVWatermark.EMPTY : MVWatermark.decode(encoded);
        }
    }

    private static MVCompiledDefinition buildFromSpec(org.opensearch.mv.MVDefinitionSpec spec) {
        // Extract group keys from the spec columns (first groupKeys count)
        List<org.opensearch.mv.GroupKey> keys = new java.util.ArrayList<>();
        for (int i = 0; i < spec.groupKeys(); i++) {
            org.opensearch.mv.MVDefinitionSpec.Column col = spec.columns().get(i);
            org.opensearch.mv.GroupKey.ColumnType type = col.type() == org.opensearch.mv.MVDefinitionSpec.ColumnType.UTF8
                ? org.opensearch.mv.GroupKey.ColumnType.KEYWORD
                : org.opensearch.mv.GroupKey.ColumnType.LONG;
            keys.add(org.opensearch.mv.GroupKey.of(col.name(), type));
        }

        // Build aggregate specs from ship fields beyond the group keys
        List<org.opensearch.mv.AggregateSpec> aggs = new java.util.ArrayList<>();
        aggs.add(org.opensearch.mv.AggregateSpec.count("cnt"));

        // Add SUM for each metric column beyond group keys
        for (int i = spec.groupKeys(); i < spec.columns().size(); i++) {
            org.opensearch.mv.MVDefinitionSpec.Column col = spec.columns().get(i);
            aggs.add(org.opensearch.mv.AggregateSpec.sum(col.name(), "sum_" + col.name()));
        }

        return MVCompiledDefinition.of(keys, aggs);
    }

    /** Immutable build result. */
    record MVBuildResult(boolean success, String artifactId, Map<String, Object> stats) implements BuildResult {
    }
}
