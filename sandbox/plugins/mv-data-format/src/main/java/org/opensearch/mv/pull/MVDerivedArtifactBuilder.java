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
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVDefinitionResolver;
import org.opensearch.mv.MVGroupByOrdering;
import org.opensearch.mv.MVStateDataFormat;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * MV-specific implementation of {@link DerivedArtifactBuilder}. Runs the
 * DataFusion fold over staged parquet files, validates coverage, writes the
 * Arrow state artifact, and publishes it via {@link IndexShard}.
 *
 * <p><b>Stage 3:</b> Production builds use streaming external sort → direct
 * IPC write via {@link MVBuildRuntime#buildStreamingArtifact}. The partial
 * aggregation output is wrapped in a SortExec over the FULL group-by tuple
 * from {@link MVGroupByOrdering}, then each sorted batch streams directly
 * into an Arrow IPC FileWriter. No terminal collect/concat/sort/take anywhere
 * in the production path. The build also produces and validates schema and
 * definition hashes for integrity.
 */
final class MVDerivedArtifactBuilder implements DerivedArtifactBuilder {

    private static final Logger logger = LogManager.getLogger(MVDerivedArtifactBuilder.class);

    private final IndexSettings indexSettings;
    private final MVPullSettings.Services services;
    private final String definitionName;
    private final MVCompiledDefinition compiledDefinition;
    private final MVGroupByOrdering ordering;

    /** Stage 2: managed build runtime (shared DataFusionRuntime). Lazy-initialized. */
    private volatile MVBuildRuntime buildRuntime;
    private volatile MVDataFusionReadEngine coverageReader;
    private volatile MVWatermark watermark;
    /** Compaction: background k-way merge of accumulated mv_state generations. */
    private volatile MVCompactionService compactionService;

    MVDerivedArtifactBuilder(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.services = services;

        // Build compiled definition from settings
        Settings settings = indexSettings.getSettings();

        // Stage 4: resolve the authoritative definition through the single
        // shared resolver — persisted descriptor FIRST (integrity-checked, fail
        // closed), else the legacy named compiledFor() fallback. A tampered /
        // oversize / unparseable / disagreeing descriptor throws here; because
        // this constructor runs inside the DerivedShardPoller constructor (which
        // NodeDerivedPullService wraps in a try/catch), a throw means the poller
        // is never registered and never starts — definition identity is
        // fail-closed across restarts.
        this.definitionName = MVDefinitionResolver.definitionLabel(settings);
        this.compiledDefinition = MVDefinitionResolver.resolve(settings);

        // Derive the ordering contract ONCE (immutable, thread-safe).
        this.ordering = compiledDefinition.groupByOrdering();

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

        // Stage 5: record fan-in round for metrics
        MVBuildMetrics.INSTANCE.recordFanInRound();

        // Initialize coverageReader lazily
        if (coverageReader == null) {
            coverageReader = new MVDataFusionReadEngine(shard.shardPath().getDataPath().resolve("mv_pull_work"));
        }

        // Lazy-initialize managed build runtime (Stage 2)
        if (buildRuntime == null) {
            buildRuntime = createBuildRuntime();
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

        // ── Bounded streaming rounds: cap per-round range ────────────────
        // When lag exceeds max_docs_per_round, process only a bounded chunk.
        // Each chunk emits one generation; the compaction machinery folds them.
        // Memory becomes O(chunk) instead of O(full_lag).
        Settings admissionSettings = indexSettings.getSettings();
        long maxDocsPerRound = MVPullSettings.MAX_DOCS_PER_ROUND.get(admissionSettings);
        final long snapshotWatermark = mvSnapshot.watermark();
        final long totalLag = snapshotWatermark - current.seqNo();
        final long roundWatermark;
        final boolean roundCapped;
        if (maxDocsPerRound < Long.MAX_VALUE && totalLag > maxDocsPerRound) {
            roundWatermark = current.seqNo() + maxDocsPerRound;
            roundCapped = true;
            logger.info(
                "mv_pull ROUND_START_CAPPED shard=[{}] range=({}, {}] capped_from={} "
                    + "total_lag={} max_docs_per_round={}",
                shard.shardId(),
                current.seqNo(),
                roundWatermark,
                snapshotWatermark,
                totalLag,
                maxDocsPerRound
            );
        } else {
            roundWatermark = snapshotWatermark;
            roundCapped = false;
        }

        // ── Pull-round admission gate (Stage 5, criteria H) ─────────────
        // Check sourceBytes, opsEstimate, and nativePressure against
        // configured limits. Safe defaults (Long.MAX_VALUE / 1.0) mean
        // all checks are no-ops unless the operator tunes them.

        // H1: source bytes admission
        long maxSourceBytes = MVPullSettings.MAX_SOURCE_BYTES_PER_ROUND.get(admissionSettings);
        if (maxSourceBytes < Long.MAX_VALUE) {
            long totalSourceBytes = 0;
            for (Path p : parquetFiles) {
                totalSourceBytes += Files.size(p);
            }
            if (totalSourceBytes > maxSourceBytes) {
                logger.warn(
                    "mv_pull admission rejected: sourceBytes={} exceeds limit={} for shard [{}]",
                    totalSourceBytes,
                    maxSourceBytes,
                    shard.shardId()
                );
                return new MVBuildResult(false, "admission-source-bytes-exceeded", Map.of());
            }
        }

        // H2: ops estimate admission (row count in the capped range)
        long maxOpsEstimate = MVPullSettings.MAX_OPS_ESTIMATE_PER_ROUND.get(admissionSettings);
        if (maxOpsEstimate < Long.MAX_VALUE) {
            long opsEstimate = roundWatermark - current.seqNo();
            if (opsEstimate > maxOpsEstimate) {
                logger.warn(
                    "mv_pull admission rejected: opsEstimate={} exceeds limit={} for shard [{}]",
                    opsEstimate,
                    maxOpsEstimate,
                    shard.shardId()
                );
                return new MVBuildResult(false, "admission-ops-estimate-exceeded", Map.of());
            }
        }

        // H3: native pressure admission (jemalloc RSS fraction)
        double maxNativePressure = MVPullSettings.MAX_NATIVE_PRESSURE_FRACTION.get(admissionSettings);
        if (maxNativePressure < 1.0) {
            try {
                long rssBytes = org.opensearch.nativebridge.spi.NativeMemoryFetcher.fetchResidentBytes();
                CircuitBreaker breaker = services.parentCircuitBreaker();
                if (rssBytes > 0 && breaker != null && breaker.getLimit() > 0) {
                    double pressure = (double) rssBytes / breaker.getLimit();
                    if (pressure > maxNativePressure) {
                        logger.warn(
                            "mv_pull admission rejected: nativePressure={} exceeds limit={} for shard [{}]",
                            pressure,
                            maxNativePressure,
                            shard.shardId()
                        );
                        return new MVBuildResult(false, "admission-native-pressure-exceeded", Map.of());
                    }
                }
            } catch (Exception e) {
                // NativeMemoryFetcher unavailable (e.g. no native library);
                // fail-open — admission is best-effort, not a hard gate.
                logger.debug("mv_pull native pressure check unavailable, skipping", e);
            }
        }

        // Use compiled definition to build SQL — capped to roundWatermark
        String baseSql = compiledDefinition.buildPartialSql(MVConstants.INPUT_TABLE);
        String filteredSql = wrapWithSeqNoFilter(baseSql, current.seqNo(), roundWatermark);

        // Coverage is schema-agnostic: count every source row in the range and
        // reduce all partial MAX(_seq_no) rows.
        long tCoverage = System.nanoTime();
        MVDataFusionReadEngine.Delta coverage = coverageReader.searchDeltaByDefinition(
            parquetFiles,
            definitionName,
            current.seqNo(),
            roundWatermark,
            mvSnapshot.infosVersion()
        );
        long coverageNanos = System.nanoTime() - tCoverage;

        if (coverage.observedMaxSeqNo() < 0L) {
            return new MVBuildResult(false, "no-coverage", Map.of());
        }
        long appliedThrough = Math.min(coverage.observedMaxSeqNo(), roundWatermark);

        // H4: cardinality estimate admission (post-coverage, uses totalRows
        // as upper bound on distinct group keys). Safe default Long.MAX_VALUE.
        long maxCardinalityEstimate = MVPullSettings.MAX_CARDINALITY_ESTIMATE_PER_ROUND.get(admissionSettings);
        if (maxCardinalityEstimate < Long.MAX_VALUE && coverage.totalRows() > maxCardinalityEstimate) {
            logger.warn(
                "mv_pull admission rejected: cardinalityEstimate={} exceeds limit={} for shard [{}]",
                coverage.totalRows(),
                maxCardinalityEstimate,
                shard.shardId()
            );
            return new MVBuildResult(false, "admission-cardinality-estimate-exceeded", Map.of());
        }

        // Coverage integrity guard
        long expectedRows = appliedThrough - current.seqNo();
        // Defect 13: count noops in the round range to adjust expected rows.
        // Noops are seqNos that consumed a sequence number but produced no
        // parquet row (failed index ops, deletes).
        long[] noopSeqNos = mvSnapshot.noopSeqNos();
        int noopsInRange = MVWatermark.countNoopsInRange(noopSeqNos, current.seqNo(), appliedThrough);
        long adjustedExpected = expectedRows - noopsInRange;
        if (MVWatermark.hasCompleteCoverage(current.seqNo(), appliedThrough, coverage.totalRows(), noopsInRange) == false) {
            logger.warn(
                "mv_pull coverage mismatch: range=({}, {}] expected={} (adjusted={}, noops={}) but found={}",
                current.seqNo(),
                appliedThrough,
                expectedRows,
                adjustedExpected,
                noopsInRange,
                coverage.totalRows()
            );
            return new MVBuildResult(false, "coverage-mismatch", Map.of());
        }
        if (noopsInRange > 0) {
            logger.info(
                "mv_pull COVERAGE shard=[{}] range=({}, {}] expected={} noops={} adjusted={} found={}",
                shard.shardId(),
                current.seqNo(),
                appliedThrough,
                expectedRows,
                noopsInRange,
                adjustedExpected,
                coverage.totalRows()
            );
        }

        // Build artifact through the MANAGED runtime (Stage 2)
        long generation = shard.reserveDerivedArtifactGeneration();
        Path stagedParquet = coverageReader.stageParquetFiles(parquetFiles, generation);
        try {
            // ── NATIVE_BUILD_PRE log (instrumentation point 4) ───────
            logger.info(
                "mv_pull NATIVE_BUILD_PRE shard=[{}] generation={} parquet_files={} "
                    + "pool_limit={}B mem_estimate={}B breaker={} range=({}, {}] coverage_rows={} capped={}",
                shard.shardId(),
                generation,
                parquetFiles.size(),
                buildRuntime != null ? buildRuntime.runtimePtr() : -1,
                MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.get(indexSettings.getSettings()),
                services.parentCircuitBreaker() != null
                    ? services.parentCircuitBreaker().getName() + "/"
                        + services.parentCircuitBreaker().getUsed() + "/"
                        + services.parentCircuitBreaker().getLimit()
                    : "none",
                current.seqNo(),
                roundWatermark,
                coverage.totalRows(),
                roundCapped
            );
            long tNativeBuild = System.nanoTime();
            ManagedArtifact artifact = buildManagedArtifact(
                stagedParquet,
                MVConstants.INPUT_TABLE,
                filteredSql,
                shard.shardPath().getDataPath(),
                generation
            );
            long nativeBuildNanos = System.nanoTime() - tNativeBuild;

            // ── NATIVE_BUILD_POST log (instrumentation point 4) ──────
            // The artifact carries the full ArtifactResult from the streaming
            // build path. Log ALL fields from buildStreamingArtifact at INFO
            // since these are the most important diagnostics for OOM/spill.
            logger.info(
                "mv_pull NATIVE_BUILD_POST shard=[{}] generation={} rows={} "
                    + "native_build_ms={} schema_hash={} definition_hash={}",
                shard.shardId(),
                generation,
                artifact.stateRows(),
                nativeBuildNanos / 1_000_000,
                Long.toHexString(artifact.schemaHash()),
                Long.toHexString(artifact.definitionHash())
            );

            MVWatermark next = new MVWatermark(mvSnapshot.primaryTerm(), appliedThrough, mvSnapshot.infosVersion());

            // Publish (commit artifact to shard metadata)
            long tPublish = System.nanoTime();
            shard.publishDerivedArtifact(
                MVStateDataFormat.INSTANCE,
                artifact.fileSet(),
                Map.of(MVWatermark.key(shard.shardId().id()), next.encode())
            );
            long publishNanos = System.nanoTime() - tPublish;
            watermark = next;

            // Register the pre-computed checksum on the shard's shared strategy
            // AFTER publish so the upload path can serve it in O(1).
            // One sequential read of the new ~500 MB generation (~2s) — eliminates
            // repeated O(n) scans on every publish and restart recovery.
            MVStateChecksumUtil.computeAndRegister(
                artifact.path(),
                artifact.path().getFileName().toString(),
                generation,
                shard
            );

            logger.info(
                "mv_pull published generation={} rows={} range=({}, {}] watermark={} "
                    + "coverage={}ms native_build={}ms publish={}ms schema_hash={} capped={} remaining_lag={} [streaming]",
                generation,
                artifact.stateRows(),
                current.seqNo(),
                appliedThrough,
                next,
                coverageNanos / 1_000_000,
                nativeBuildNanos / 1_000_000,
                publishNanos / 1_000_000,
                Long.toHexString(artifact.schemaHash()),
                roundCapped,
                roundCapped ? (snapshotWatermark - appliedThrough) : 0L
            );

            Map<String, Object> stats = new java.util.LinkedHashMap<>();
            stats.put("stateRows", artifact.stateRows());
            stats.put("generation", generation);
            stats.put("coverage_check_nanos", coverageNanos);
            stats.put("native_build_nanos", nativeBuildNanos);
            stats.put("publish_nanos", publishNanos);
            stats.put("parquet_files", (long) parquetFiles.size());
            stats.put("applied_range", appliedThrough - current.seqNo());
            stats.put("managed", true); // Stage 2 marker
            stats.put("streaming", true); // Stage 3 marker: no collect/concat/sort/take
            stats.put("schema_hash", Long.toHexString(artifact.schemaHash()));
            stats.put("definition_hash", Long.toHexString(artifact.definitionHash()));
            // Bounded streaming round metadata: the poller reads these to decide
            // whether to advance to the capped watermark and continue immediately.
            stats.put("capped", roundCapped);
            stats.put("capped_watermark", appliedThrough);
            stats.put("snapshot_watermark", snapshotWatermark);
            stats.put("remaining_lag", roundCapped ? (snapshotWatermark - appliedThrough) : 0L);

            // ── Compaction: trigger background merge of accumulated generations ──
            try {
                if (compactionService == null) {
                    compactionService = new MVCompactionService(compiledDefinition, services.threadPool());
                }
                int threshold = MVPullSettings.MAX_GENERATIONS_BEFORE_COMPACT.get(indexSettings.getSettings());
                compactionService.maybeCompact(shard, threshold);
            } catch (Exception compactEx) {
                // Never fail a successful build because of compaction scheduling
                logger.warn("mv_pull compaction trigger failed for shard [{}]", shard.shardId(), compactEx);
            }

            return new MVBuildResult(true, "gen-" + generation, stats);
        } finally {
            coverageReader.cleanupStagedParquet(stagedParquet);
        }
    }

    /**
     * Stage 3: Build the MV state artifact through the streaming managed runtime.
     * Replaces the Stage 2 collect→concat→sort→take path with streaming
     * external sort → direct IPC write.
     *
     * <p>The native writer targets a private temporary path. The completed
     * file is atomically renamed before the result is returned, so callers
     * can never publish a partially-written artifact.
     *
     * <p>Validates schema hash and definition hash against the compiled
     * definition for integrity.
     */
    private ManagedArtifact buildManagedArtifact(
        Path parquetInput,
        String tableName,
        String filteredSql,
        Path outputRoot,
        long writerGeneration
    ) throws IOException {
        Path formatDirectory = outputRoot.resolve(MVStateDataFormat.NAME);
        Files.createDirectories(formatDirectory);
        String fileName = MVConstants.mvFileName(writerGeneration);
        Path completed = formatDirectory.resolve(fileName);
        Path temporary = formatDirectory.resolve(fileName + ".tmp-" + UUID.randomUUID());

        boolean success = false;
        boolean completedCreated = false;
        try {
            // Stage 3: streaming build with metadata validation
            MVBuildRuntime.ArtifactResult artifactResult = buildRuntime.buildStreamingArtifact(
                parquetInput.toString(),
                tableName,
                filteredSql,
                temporary.toString(),
                ordering
            );
            long stateRows = artifactResult.rowCount();
            if (stateRows <= 0L) {
                throw new IOException("mv_pull streaming build produced no state rows for generation [" + writerGeneration + "]");
            }

            // Log artifact metadata including native hashes
            logger.debug(
                "mv_pull artifact metadata: generation={} rows={} schemaHash={} definitionHash={} orderingHash={}",
                writerGeneration,
                stateRows,
                Long.toHexString(artifactResult.schemaHash()),
                Long.toHexString(artifactResult.definitionHash()),
                Long.toHexString(artifactResult.orderingHash())
            );

            moveCompletedArtifact(temporary, completed);
            completedCreated = true;

            WriterFileSet fileSet = MonoFileWriterSet.of(formatDirectory.toAbsolutePath(), writerGeneration, fileName, stateRows);
            success = true;
            return new ManagedArtifact(completed, fileSet, stateRows, artifactResult.schemaHash(), artifactResult.definitionHash());
        } finally {
            Files.deleteIfExists(temporary);
            if (success == false && completedCreated) {
                Files.deleteIfExists(completed);
            }
        }
    }

    /**
     * Create the managed build runtime from services.
     * Sources the DataFusionRuntime pointer and circuit breaker from the node-level services.
     */
    private MVBuildRuntime createBuildRuntime() {
        Settings settings = indexSettings.getSettings();
        long spillBytes = MVBuildRuntime.MV_SPILL_BUDGET_BYTES.get(settings);
        int spillFiles = MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.get(settings);
        long memEstimate = MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.get(settings);

        long runtimePtr = services.dataFusionRuntimePtr();
        CircuitBreaker breaker = services.parentCircuitBreaker();

        logger.info(
            "mv_pull creating MVBuildRuntime: runtimePtr={}, spillBudget={}B, spillFiles={}, memEstimate={}B, breaker={}",
            runtimePtr,
            spillBytes,
            spillFiles,
            memEstimate,
            breaker != null ? breaker.getName() : "none"
        );

        return new MVBuildRuntime(runtimePtr, spillBytes, spillFiles, memEstimate, breaker);
    }

    @Override
    public void close() throws IOException {
        if (compactionService != null) {
            compactionService.close();
        }
        if (buildRuntime != null) {
            buildRuntime.close();
        }
        if (coverageReader != null) {
            coverageReader.close();
        }
    }

    private static String wrapWithSeqNoFilter(String baseSql, long fromExclusive, long toInclusive) {
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
            var userData = ref.get().getUserData();
            String key = MVWatermark.key(sourceShardId);
            String encoded = userData.get(key);
            if (encoded == null) {
                logger.warn(
                    "mv_pull WATERMARK_RECOVERY shard=[{}] source_shard={} key=[{}] "
                        + "result=EMPTY (no entry in commit userData; available keys={})",
                    shard.shardId(),
                    sourceShardId,
                    key,
                    userData.keySet()
                );
                return MVWatermark.EMPTY;
            }
            MVWatermark wm = MVWatermark.decode(encoded);
            logger.info(
                "mv_pull WATERMARK_RECOVERY shard=[{}] source_shard={} key=[{}] "
                    + "recovered={} (term={} seqNo={} gen={})",
                shard.shardId(),
                sourceShardId,
                key,
                encoded,
                wm.primaryTerm(),
                wm.seqNo(),
                wm.generation()
            );
            return wm;
        } catch (Exception e) {
            logger.error(
                "mv_pull WATERMARK_RECOVERY shard=[{}] source_shard={} FAILED: {}",
                shard.shardId(),
                sourceShardId,
                e.getMessage(),
                e
            );
            return MVWatermark.EMPTY;
        }
    }

    private static void moveCompletedArtifact(Path temporary, Path completed) throws IOException {
        try {
            Files.move(temporary, completed, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, completed);
        }
    }

    /** Immutable build result. */
    record MVBuildResult(boolean success, String artifactId, Map<String, Object> stats) implements BuildResult {
    }

    /** One completed managed artifact and its metadata. */
    record ManagedArtifact(Path path, WriterFileSet fileSet, long stateRows, long schemaHash, long definitionHash) {
    }
}
