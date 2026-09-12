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
import org.opensearch.mv.MVBuildActivity;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVDefinitionResolver;
import org.opensearch.mv.MVGroupByOrdering;

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

    /**
     * Adaptive per-round cap. Halved (down to a floor) after a native build failed for lack of memory — the
     * DataFusion external sort of a large catch-up round — and doubled back toward the configured
     * {@code index.mv_pull.max_docs_per_round} after every successful round. Observed 2026-09-10: after a run of
     * coverage retries the accumulated lag produced a 2M-document round that failed with "Not enough memory to
     * continue external sort", and the retry only succeeded a minute later.
     */
    private volatile long adaptiveCapDocs = Long.MAX_VALUE;
    private static final long ADAPTIVE_CAP_FLOOR_DOCS = Long.getLong("opensearch.mv_pull.adaptive_cap_floor_docs", 250_000L);

    private static boolean isMemoryExhaustion(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            String m = c.getMessage();
            if (m != null && (m.contains("Not enough memory") || m.contains("Resources exhausted") || m.contains("memory_limit"))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
        long configuredCap = MVPullSettings.MAX_DOCS_PER_ROUND.get(indexSettings.getSettings());
        long capUsed = Math.min(configuredCap, adaptiveCapDocs);
        try {
            BuildResult result = buildOnce(snapshot, stageDir, shard);
            if (result != null && result.success() && adaptiveCapDocs < configuredCap) {
                adaptiveCapDocs = Math.min(configuredCap, Math.max(ADAPTIVE_CAP_FLOOR_DOCS, adaptiveCapDocs) * 2);
                logger.info(
                    "mv_pull ADAPTIVE_CAP shard=[{}] restored to {} docs/round (configured {})",
                    shard.shardId(),
                    adaptiveCapDocs,
                    configuredCap
                );
            }
            return result;
        } catch (Exception e) {
            if (isMemoryExhaustion(e)) {
                long next = Math.max(ADAPTIVE_CAP_FLOOR_DOCS, Math.min(configuredCap, capUsed) / 2);
                adaptiveCapDocs = next;
                logger.warn(
                    "mv_pull ADAPTIVE_CAP shard=[{}] native build ran out of memory with {} docs/round; next round capped at {} docs: {}",
                    shard.shardId(),
                    capUsed,
                    next,
                    e.getMessage() == null
                        ? e.getClass().getSimpleName()
                        : e.getMessage().substring(0, Math.min(160, e.getMessage().length()))
                );
            }
            throw e;
        }
    }

    private BuildResult buildOnce(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
        // Defect #23 admission signal: this round is about to hold native pool
        // memory (coverage scan + streaming build). While the claim is active,
        // merge-admission gating admits no new merges for this shard
        // — merges stall, live MV ingestion never does. The claim ALWAYS
        // resolves (try/finally); the final round of a catch-up burst resolves
        // it early, before publishing, so the publication's merge trigger
        // re-admits deferred merges exactly at the transition to quiet.
        MVBuildActivity.markActive(shard.shardId().getIndexName(), shard.shardId().id());
        buildPressureCleared = false;
        try {
            return buildUnderPressureClaim(snapshot, stageDir, shard);
        } finally {
            if (buildPressureCleared == false) {
                MVBuildActivity.clearActive(shard.shardId().getIndexName(), shard.shardId().id());
            }
        }
    }

    /**
     * Set when the round released its build-pressure claim early (final round
     * of a burst, before publish). Poller rounds are single-threaded per
     * builder, so a plain field is safe.
     */
    private boolean buildPressureCleared;

    private BuildResult buildUnderPressureClaim(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
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
        long maxDocsPerRound = Math.min(MVPullSettings.MAX_DOCS_PER_ROUND.get(admissionSettings), adaptiveCapDocs);
        final long snapshotWatermark = mvSnapshot.watermark();
        final long totalLag = snapshotWatermark - current.seqNo();
        final long roundWatermark;
        final boolean roundCapped;
        if (maxDocsPerRound < Long.MAX_VALUE && totalLag > maxDocsPerRound) {
            roundWatermark = current.seqNo() + maxDocsPerRound;
            roundCapped = true;
            logger.info(
                "mv_pull ROUND_START_CAPPED shard=[{}] range=({}, {}] capped_from={} " + "total_lag={} max_docs_per_round={}",
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

        // A round that applies only a PREFIX of the snapshot range (e.g. some
        // advertised files were not yet visible in the remote store and were
        // skipped by staging) is EFFECTIVELY CAPPED: the poller must resume
        // from appliedThrough next round, never fast-forward to the snapshot
        // watermark — that would silently skip (appliedThrough, snapshot].
        final boolean effectivelyCapped = roundCapped || appliedThrough < snapshotWatermark;
        if (effectivelyCapped && roundCapped == false) {
            logger.info(
                "mv_pull ROUND_TRUNCATED shard=[{}] applied_through={} snapshot_watermark={} "
                    + "(partial staging — resuming from applied prefix next round)",
                shard.shardId(),
                appliedThrough,
                snapshotWatermark
            );
        }

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
                    ? services.parentCircuitBreaker().getName()
                        + "/"
                        + services.parentCircuitBreaker().getUsed()
                        + "/"
                        + services.parentCircuitBreaker().getLimit()
                    : "none",
                current.seqNo(),
                roundWatermark,
                coverage.totalRows(),
                effectivelyCapped
            );
            long tNativeBuild = System.nanoTime();
            // One native job per shard at a time: a compaction of this shard's
            // generations (MVStateCompactionMerger) shares this lock.
            java.util.concurrent.locks.ReentrantLock buildLock = MVShardBuildLock.forShard(shard.shardId());
            buildLock.lock();
            final ManagedArtifact artifact;
            try {
                artifact = buildManagedArtifact(
                    stagedParquet,
                    MVConstants.INPUT_TABLE,
                    filteredSql,
                    shard.shardPath().getDataPath(),
                    generation
                );
            } finally {
                buildLock.unlock();
            }
            long nativeBuildNanos = System.nanoTime() - tNativeBuild;

            // ── NATIVE_BUILD_POST log (instrumentation point 4) ──────
            // The artifact carries the full ArtifactResult from the streaming
            // build path. Log ALL fields from buildStreamingArtifact at INFO
            // since these are the most important diagnostics for OOM/spill.
            logger.info(
                "mv_pull NATIVE_BUILD_POST shard=[{}] generation={} rows={} " + "native_build_ms={} schema_hash={} definition_hash={}",
                shard.shardId(),
                generation,
                artifact.stateRows(),
                nativeBuildNanos / 1_000_000,
                Long.toHexString(artifact.schemaHash()),
                Long.toHexString(artifact.definitionHash())
            );

            MVWatermark next = new MVWatermark(mvSnapshot.primaryTerm(), appliedThrough, mvSnapshot.infosVersion());

            if (effectivelyCapped == false) {
                // Final round of this catch-up burst: the native build has
                // released its pool memory and no further round is imminent.
                // Release the build-pressure claim BEFORE publishing so the
                // publication's merge trigger (DataFormatAwareEngine
                // publishDerivedArtifact -> triggerPossibleMerges) can admit
                // the merges deferred while builds were running. Capped rounds
                // keep the claim through their publish — their trigger fires
                // rejected, and the next round re-marks — so merges stay
                // stalled across sustained catch-up.
                MVBuildActivity.clearActive(shard.shardId().getIndexName(), shard.shardId().id());
                buildPressureCleared = true;
            }

            // Publish (commit artifact to shard metadata)
            long tPublish = System.nanoTime();
            shard.publishDerivedArtifact(
                MVConstants.STATE_ARTIFACT_FORMAT,
                artifact.fileSet(),
                Map.of(MVWatermark.key(shard.shardId().id()), next.encode())
            );
            long publishNanos = System.nanoTime() - tPublish;
            watermark = next;

            // Register the pre-computed checksum on the shard's shared strategy
            // AFTER publish so the upload path can serve it in O(1).
            // One sequential read of the new ~500 MB generation (~2s) — eliminates
            // repeated O(n) scans on every publish and restart recovery.
            MVStateChecksumUtil.computeAndRegister(artifact.path(), artifact.path().getFileName().toString(), generation, shard);

            // ── Builder-shard emulation: fold every follower view over the same staged
            // files and hand each its state through its outbox. Runs after this
            // leader's own publish so the leader's lag is the zero-hop reference.
            Map<String, Object> fanOutStats = fanOutToFollowers(stagedParquet, current.seqNo(), appliedThrough, mvSnapshot, shard);

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
            stats.put("capped", effectivelyCapped);
            stats.put("capped_watermark", appliedThrough);
            stats.put("snapshot_watermark", snapshotWatermark);
            stats.put("remaining_lag", effectivelyCapped ? (snapshotWatermark - appliedThrough) : 0L);
            stats.putAll(fanOutStats);

            return new MVBuildResult(true, "gen-" + generation, stats);
        } finally {
            coverageReader.cleanupStagedParquet(stagedParquet);
        }
    }

    // ── Builder-shard emulation: leader fan-out ─────────────────────────────

    /** A hydrating follower of this leader: its compiled definition, ordering, outbox and publication cursor. */
    private static final class Follower {
        final String index;
        final MVCompiledDefinition definition;
        final MVGroupByOrdering ordering;
        MVBuilderOutbox outbox;
        long lastTo = Long.MIN_VALUE; // MIN_VALUE = cursor not yet initialised from the outbox
        boolean broken;

        Follower(String index, MVCompiledDefinition definition) {
            this.index = index;
            this.definition = definition;
            this.ordering = definition.groupByOrdering();
        }
    }

    private final Map<String, Follower> followers = new java.util.LinkedHashMap<>();
    private long followersMetadataVersion = -1L;

    /** Re-read the follower set when cluster metadata changed: targets in hydrate mode naming this leader. */
    private void refreshFollowers() {
        org.opensearch.cluster.ClusterState state = services.clusterService().state();
        long version = state.metadata().version();
        if (version == followersMetadataVersion) {
            return;
        }
        String leaderIndex = indexSettings.getIndex().getName();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (org.opensearch.cluster.metadata.IndexMetadata im : state.metadata().indices().values()) {
            Settings s = im.getSettings();
            if (MVPullSettings.MODE_HYDRATE.equals(MVPullSettings.PULL_MODE.get(s)) == false
                || leaderIndex.equals(MVPullSettings.BUILDER_VIEW.get(s)) == false) {
                continue;
            }
            String name = im.getIndex().getName();
            seen.add(name);
            if (followers.containsKey(name) == false) {
                try {
                    followers.put(name, new Follower(name, MVDefinitionResolver.resolve(s)));
                    logger.info("mv_pull FOLLOWER_ATTACH leader=[{}] follower=[{}]", leaderIndex, name);
                } catch (RuntimeException e) {
                    logger.error("mv_pull FOLLOWER_REJECTED leader=[{}] follower=[{}]: {}", leaderIndex, name, e.getMessage());
                }
            }
        }
        followers.keySet().removeIf(name -> seen.contains(name) == false);
        followersMetadataVersion = version;
    }

    /**
     * Fold every follower's definition over the leader's staged files for
     * {@code (fromExclusive, toInclusive]} and publish each result to that
     * follower's outbox. Never fails the leader's round; a follower whose fold
     * or upload fails is marked broken and no longer fed (its lag then grows
     * visibly) rather than being handed a chain with a gap.
     */
    private Map<String, Object> fanOutToFollowers(
        Path stagedParquet,
        long fromExclusive,
        long toInclusive,
        MVDerivedSourceReader.MVSourceSnapshot mvSnapshot,
        IndexShard shard
    ) {
        Map<String, Object> stats = new java.util.LinkedHashMap<>();
        try {
            refreshFollowers();
        } catch (RuntimeException e) {
            logger.warn("mv_pull FANOUT follower discovery failed for shard [{}]: {}", shard.shardId(), e.getMessage());
        }
        if (followers.isEmpty()) {
            return stats;
        }
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        int sourceShardId = binding != null ? binding.resolveSourceShard(shard.shardId().id()) : shard.shardId().id();
        String leaderIndex = indexSettings.getIndex().getName();
        long nativeNanos = 0L;
        long uploadNanos = 0L;
        long rowsTotal = 0L;
        int published = 0;
        int broken = 0;
        Path outDir = shard.shardPath().getDataPath().resolve("mv_builder_outbox");

        for (Follower f : followers.values()) {
            if (f.broken) {
                broken++;
                continue;
            }
            Path tmp = null;
            try {
                if (f.outbox == null) {
                    org.opensearch.cluster.metadata.IndexMetadata source = services.sourceIndexMetadata(binding.sourceName());
                    String repository = source.getSettings()
                        .get(org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
                    f.outbox = MVBuilderOutbox.open(
                        services.repositoriesService(),
                        repository,
                        source.getIndexUUID(),
                        sourceShardId,
                        f.index
                    );
                    MVBuilderOutbox.Publication latest = f.outbox.latest();
                    f.lastTo = latest == null ? -1L : latest.toInclusive();
                }
                if (f.lastTo >= 0 && f.lastTo != fromExclusive) {
                    // We cannot fold (lastTo, fromExclusive] from this round's staging: those rows are not staged.
                    logger.error(
                        "mv_pull FOLLOWER_BROKEN leader=[{}] follower=[{}] last_published_to={} this_round_from={} — cursor gap, stop feeding",
                        leaderIndex,
                        f.index,
                        f.lastTo,
                        fromExclusive
                    );
                    f.broken = true;
                    broken++;
                    continue;
                }
                Files.createDirectories(outDir.resolve(f.index));
                tmp = outDir.resolve(f.index).resolve(MVBuilderOutbox.Publication.stateBlobFor(toInclusive) + ".tmp-" + UUID.randomUUID());
                String sql = wrapWithSeqNoFilter(f.definition.buildPartialSql(MVConstants.INPUT_TABLE), fromExclusive, toInclusive);

                long t0 = System.nanoTime();
                java.util.concurrent.locks.ReentrantLock buildLock = MVShardBuildLock.forShard(shard.shardId());
                buildLock.lock();
                final long rows;
                try {
                    rows = buildRuntime.buildStreamingArtifact(
                        stagedParquet.toString(),
                        MVConstants.INPUT_TABLE,
                        sql,
                        tmp.toString(),
                        f.ordering
                    ).rowCount();
                } finally {
                    buildLock.unlock();
                }
                nativeNanos += System.nanoTime() - t0;
                if (rows <= 0L) {
                    throw new IOException("follower fold produced no state rows for range (" + fromExclusive + ", " + toInclusive + "]");
                }

                long t1 = System.nanoTime();
                f.outbox.publish(
                    tmp,
                    fromExclusive,
                    toInclusive,
                    mvSnapshot.primaryTerm(),
                    mvSnapshot.infosVersion(),
                    rows,
                    f.lastTo,
                    leaderIndex
                );
                uploadNanos += System.nanoTime() - t1;
                f.lastTo = toInclusive;
                rowsTotal += rows;
                published++;
            } catch (Exception e) {
                f.broken = true;
                broken++;
                logger.error(
                    "mv_pull FOLLOWER_BROKEN leader=[{}] follower=[{}] range=({}, {}] stop feeding: {}",
                    leaderIndex,
                    f.index,
                    fromExclusive,
                    toInclusive,
                    e.getMessage(),
                    e
                );
            } finally {
                if (tmp != null) {
                    try {
                        Files.deleteIfExists(tmp);
                    } catch (IOException ignored) {
                        // best effort
                    }
                }
            }
        }
        logger.info(
            "mv_pull FANOUT shard=[{}] range=({}, {}] followers={} published={} broken={} rows={} native_ms={} upload_ms={}",
            shard.shardId(),
            fromExclusive,
            toInclusive,
            followers.size(),
            published,
            broken,
            rowsTotal,
            nativeNanos / 1_000_000,
            uploadNanos / 1_000_000
        );
        stats.put("fanout_followers", (long) followers.size());
        stats.put("fanout_published", (long) published);
        stats.put("fanout_broken", (long) broken);
        stats.put("fanout_rows", rowsTotal);
        stats.put("fanout_native_nanos", nativeNanos);
        stats.put("fanout_upload_nanos", uploadNanos);
        return stats;
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
        // State artifacts are STOCK PARQUET files owned by the target's
        // composite primary: same directory, checksum strategy, catalog
        // keying, and upload path as any parquet generation.
        Path formatDirectory = outputRoot.resolve(MVConstants.STATE_ARTIFACT_FORMAT);
        Files.createDirectories(formatDirectory);
        String fileName = MVConstants.stateFileName(writerGeneration);
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

    static MVWatermark recoveredWatermark(IndexShard shard, int sourceShardId) throws IOException {
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
                "mv_pull WATERMARK_RECOVERY shard=[{}] source_shard={} key=[{}] " + "recovered={} (term={} seqNo={} gen={})",
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

    static void moveCompletedArtifact(Path temporary, Path completed) throws IOException {
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
