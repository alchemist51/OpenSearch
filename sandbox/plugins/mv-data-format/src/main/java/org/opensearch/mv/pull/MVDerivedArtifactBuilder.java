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
        long roundWatermark;
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
        // D2 measurement: how far the source's global checkpoint trails what the remote store already exposes.
        final long globalCheckpoint = mvSnapshot.globalCheckpoint();
        final boolean boundToGcp = MVPullSettings.BOUND_TO_GLOBAL_CHECKPOINT.get(admissionSettings);
        if (globalCheckpoint >= 0) {
            logger.info(
                "mv_pull GCP shard=[{}] snapshot_max_seqno={} global_checkpoint={} gcp_lag={} bound_enabled={}",
                shard.shardId(),
                snapshotWatermark,
                globalCheckpoint,
                snapshotWatermark - globalCheckpoint,
                boundToGcp
            );
        }
        if (boundToGcp && globalCheckpoint >= 0 && globalCheckpoint < roundWatermark) {
            if (globalCheckpoint <= current.seqNo()) {
                return new MVBuildResult(false, "gcp-not-advanced", Map.of("global_checkpoint", globalCheckpoint));
            }
            roundWatermark = globalCheckpoint;
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
            Map<String, Object> fanOutStats = fanOutToFollowers(
                stagedParquet,
                parquetFiles,
                current.seqNo(),
                appliedThrough,
                mvSnapshot,
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
        /** Rows produced by the last fold — the cost estimate for cheapest-first scheduling (0 = unknown, goes first). */
        volatile long lastRows;
        /** D1: push each publication to the follower primary over transport (else the follower polls the outbox). */
        final boolean push;
        String sourceIndexUuid;

        Follower(String index, MVCompiledDefinition definition, boolean push) {
            this.index = index;
            this.definition = definition;
            this.ordering = definition.groupByOrdering();
            this.push = push;
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
                    boolean push = MVPullSettings.TRANSPORT_PUSH.equals(MVPullSettings.HYDRATE_TRANSPORT.get(s));
                    followers.put(name, new Follower(name, MVDefinitionResolver.resolve(s), push));
                    logger.info(
                        "mv_pull FOLLOWER_ATTACH leader=[{}] follower=[{}] transport={}",
                        leaderIndex,
                        name,
                        push ? "push" : "poll"
                    );
                } catch (RuntimeException e) {
                    logger.error("mv_pull FOLLOWER_REJECTED leader=[{}] follower=[{}]: {}", leaderIndex, name, e.getMessage());
                }
            }
        }
        followers.keySet().removeIf(name -> seen.contains(name) == false);
        followersMetadataVersion = version;
    }

    /** Per-follower state for ordering: rows produced by its last fold (cheapest-first scheduling). */
    private final java.util.concurrent.atomic.AtomicReference<java.util.concurrent.Future<?>> fanOutInFlight =
        new java.util.concurrent.atomic.AtomicReference<>();
    private volatile java.util.List<MVBuildRuntime> fanOutRuntimes;

    private java.util.List<MVBuildRuntime> fanOutRuntimes(int slots) {
        java.util.List<MVBuildRuntime> r = fanOutRuntimes;
        if (r == null || r.size() < slots) {
            r = new java.util.ArrayList<>();
            for (int i = 0; i < slots; i++) {
                r.add(createBuildRuntime());
            }
            fanOutRuntimes = r;
        }
        return r;
    }

    /**
     * Fold every follower's definition over the leader's staged files for
     * {@code (fromExclusive, toInclusive]} and publish each result to that
     * follower's outbox. Never fails the leader's round; a follower whose fold
     * or upload fails is marked broken and no longer fed (its lag then grows
     * visibly) rather than being handed a chain with a gap.
     *
     * <p>Fan-out v2: followers run cheapest-first with {@code fanout_concurrency}
     * folds in flight (each on its own native runtime handle, outside the shard
     * build lock); with {@code fanout_async} the staged source files are moved to a
     * hand-off directory and the fan-out runs on the generic pool while the leader
     * starts its next round. At most one fan-out is in flight so every follower
     * receives its publications in order; a round whose predecessor is still
     * fanning out waits for it (logged as FANOUT_BACKPRESSURE).
     */
    private Map<String, Object> fanOutToFollowers(
        Path stagedParquet,
        java.util.List<Path> parquetFiles,
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
        Settings settings = indexSettings.getSettings();
        final int concurrency = MVPullSettings.FANOUT_CONCURRENCY.get(settings);
        final boolean async = MVPullSettings.FANOUT_ASYNC.get(settings);
        final boolean cheapestFirst = "discovery".equals(MVPullSettings.FANOUT_ORDER.get(settings)) == false;
        final long primaryTerm = mvSnapshot.primaryTerm();
        final long infosVersion = mvSnapshot.infosVersion();

        // Ordering guarantee: wait for the previous fan-out before starting this one.
        java.util.concurrent.Future<?> previous = fanOutInFlight.get();
        if (previous != null && previous.isDone() == false) {
            long tw = System.nanoTime();
            try {
                previous.get(10, java.util.concurrent.TimeUnit.MINUTES);
            } catch (Exception e) {
                logger.warn("mv_pull FANOUT previous fan-out did not finish cleanly: {}", e.toString());
            }
            long waitedMs = (System.nanoTime() - tw) / 1_000_000;
            stats.put("fanout_backpressure_ms", waitedMs);
            logger.info("mv_pull FANOUT_BACKPRESSURE shard=[{}] waited_ms={} (previous fan-out still running)", shard.shardId(), waitedMs);
        }

        java.util.List<Follower> order = new java.util.ArrayList<>();
        for (Follower f : followers.values()) {
            if (f.broken == false) {
                order.add(f);
            }
        }
        if (cheapestFirst) {
            order.sort(java.util.Comparator.comparingLong(f -> f.lastRows));
        }
        final int broken = followers.size() - order.size();

        if (async == false) {
            Map<String, Object> r = runFanOut(
                order,
                stagedParquet,
                fromExclusive,
                toInclusive,
                primaryTerm,
                infosVersion,
                shard,
                concurrency,
                broken,
                false
            );
            stats.putAll(r);
            return stats;
        }
        // Async: take the staged source files out of the round's stage directory so the poller's cleanup cannot remove them.
        try {
            Path handoff = shard.shardPath().getDataPath().resolve("mv_builder_fanout").resolve("range-" + toInclusive);
            Files.createDirectories(handoff);
            java.util.List<Path> moved = new java.util.ArrayList<>();
            int i = 0;
            for (Path f : parquetFiles) {
                Path dest = handoff.resolve(String.format(java.util.Locale.ROOT, "%06d.parquet", i++));
                Files.move(f, dest, StandardCopyOption.REPLACE_EXISTING);
                moved.add(dest);
            }
            final Path table = handoff; // a directory of plain parquet files is a valid DataFusion listing table
            java.util.concurrent.Future<?> fut = services.threadPool().generic().submit(() -> {
                try {
                    runFanOut(order, table, fromExclusive, toInclusive, primaryTerm, infosVersion, shard, concurrency, broken, true);
                } finally {
                    for (Path m : moved) {
                        try {
                            Files.deleteIfExists(m);
                        } catch (IOException ignored) {
                            // best effort
                        }
                    }
                    try {
                        Files.deleteIfExists(handoff);
                    } catch (IOException ignored) {
                        // best effort
                    }
                }
            });
            fanOutInFlight.set(fut);
            stats.put("fanout_followers", (long) order.size());
            stats.put("fanout_async", true);
            logger.info(
                "mv_pull FANOUT_HANDOFF shard=[{}] range=({}, {}] followers={} concurrency={} files={}",
                shard.shardId(),
                fromExclusive,
                toInclusive,
                order.size(),
                concurrency,
                moved.size()
            );
        } catch (IOException e) {
            logger.error(
                "mv_pull FANOUT_HANDOFF_FAILED shard=[{}] range=({}, {}]: {}",
                shard.shardId(),
                fromExclusive,
                toInclusive,
                e.toString()
            );
        }
        return stats;
    }

    /** Fold + publish for an ordered list of followers with bounded concurrency; returns the round's fan-out stats. */
    private Map<String, Object> runFanOut(
        java.util.List<Follower> order,
        Path table,
        long fromExclusive,
        long toInclusive,
        long primaryTerm,
        long infosVersion,
        IndexShard shard,
        int concurrency,
        int brokenBefore,
        boolean async
    ) {
        long t0 = System.nanoTime();
        final java.util.concurrent.atomic.AtomicLong nativeNanos = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong uploadNanos = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicLong rowsTotal = new java.util.concurrent.atomic.AtomicLong();
        final java.util.concurrent.atomic.AtomicInteger published = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger broken = new java.util.concurrent.atomic.AtomicInteger(brokenBefore);
        int slots = Math.max(1, Math.min(concurrency, order.size()));
        java.util.List<MVBuildRuntime> runtimes = fanOutRuntimes(slots);
        java.util.concurrent.Semaphore permits = new java.util.concurrent.Semaphore(slots);
        java.util.concurrent.ConcurrentLinkedQueue<Integer> freeSlots = new java.util.concurrent.ConcurrentLinkedQueue<>();
        for (int i = 0; i < slots; i++) {
            freeSlots.add(i);
        }
        java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(order.size());
        for (Follower f : order) {
            Runnable task = () -> {
                Integer slot = null;
                try {
                    permits.acquire();
                    slot = freeSlots.poll();
                    MVBuildRuntime rt = runtimes.get(slot == null ? 0 : slot);
                    long[] res = foldAndPublishFollower(f, rt, table, fromExclusive, toInclusive, primaryTerm, infosVersion, shard);
                    if (res != null) {
                        nativeNanos.addAndGet(res[0]);
                        uploadNanos.addAndGet(res[1]);
                        rowsTotal.addAndGet(res[2]);
                        published.incrementAndGet();
                    } else {
                        broken.incrementAndGet();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                } finally {
                    if (slot != null) {
                        freeSlots.add(slot);
                    }
                    permits.release();
                    done.countDown();
                }
            };
            if (slots > 1) {
                services.threadPool().generic().execute(task);
            } else {
                task.run();
            }
        }
        try {
            done.await(30, java.util.concurrent.TimeUnit.MINUTES);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        long wallNanos = System.nanoTime() - t0;
        logger.info(
            "mv_pull FANOUT shard=[{}] range=({}, {}] followers={} published={} broken={} rows={} native_ms={} upload_ms={} wall_ms={} concurrency={} async={}",
            shard.shardId(),
            fromExclusive,
            toInclusive,
            order.size() + brokenBefore,
            published.get(),
            broken.get(),
            rowsTotal.get(),
            nativeNanos.get() / 1_000_000,
            uploadNanos.get() / 1_000_000,
            wallNanos / 1_000_000,
            slots,
            async
        );
        Map<String, Object> stats = new java.util.LinkedHashMap<>();
        stats.put("fanout_followers", (long) (order.size() + brokenBefore));
        stats.put("fanout_published", (long) published.get());
        stats.put("fanout_broken", (long) broken.get());
        stats.put("fanout_rows", rowsTotal.get());
        stats.put("fanout_native_nanos", nativeNanos.get());
        stats.put("fanout_upload_nanos", uploadNanos.get());
        stats.put("fanout_wall_nanos", wallNanos);
        return stats;
    }

    /** One follower: fold over the staged table and publish to its outbox (+ push). Returns {nativeNanos, uploadNanos, rows} or null when the follower broke. */
    private long[] foldAndPublishFollower(
        Follower f,
        MVBuildRuntime rt,
        Path table,
        long fromExclusive,
        long toInclusive,
        long primaryTerm,
        long infosVersion,
        IndexShard shard
    ) {
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        int sourceShardId = binding != null ? binding.resolveSourceShard(shard.shardId().id()) : shard.shardId().id();
        String leaderIndex = indexSettings.getIndex().getName();
        Path outDir = shard.shardPath().getDataPath().resolve("mv_builder_outbox");
        Path tmp = null;
        try {
            if (f.outbox == null) {
                org.opensearch.cluster.metadata.IndexMetadata source = services.sourceIndexMetadata(binding.sourceName());
                String repository = source.getSettings()
                    .get(org.opensearch.cluster.metadata.IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
                f.sourceIndexUuid = source.getIndexUUID();
                f.outbox = MVBuilderOutbox.open(services.repositoriesService(), repository, f.sourceIndexUuid, sourceShardId, f.index);
                MVBuilderOutbox.Publication latest = f.outbox.latest();
                f.lastTo = latest == null ? -1L : latest.toInclusive();
            }
            if (f.lastTo >= 0 && f.lastTo != fromExclusive) {
                logger.error(
                    "mv_pull FOLLOWER_BROKEN leader=[{}] follower=[{}] last_published_to={} this_round_from={} — cursor gap, stop feeding",
                    leaderIndex,
                    f.index,
                    f.lastTo,
                    fromExclusive
                );
                f.broken = true;
                return null;
            }
            Files.createDirectories(outDir.resolve(f.index));
            tmp = outDir.resolve(f.index).resolve(MVBuilderOutbox.Publication.stateBlobFor(toInclusive) + ".tmp-" + UUID.randomUUID());
            String sql = wrapWithSeqNoFilter(f.definition.buildPartialSql(MVConstants.INPUT_TABLE), fromExclusive, toInclusive);
            long t0 = System.nanoTime();
            long rows = rt.buildStreamingArtifact(table.toString(), MVConstants.INPUT_TABLE, sql, tmp.toString(), f.ordering).rowCount();
            long nativeNanos = System.nanoTime() - t0;
            if (rows <= 0L) {
                throw new IOException("follower fold produced no state rows for range (" + fromExclusive + ", " + toInclusive + "]");
            }
            long t1 = System.nanoTime();
            MVBuilderOutbox.Publication pub = f.outbox.publish(
                tmp,
                fromExclusive,
                toInclusive,
                primaryTerm,
                infosVersion,
                rows,
                f.lastTo,
                leaderIndex
            );
            long uploadNanos = System.nanoTime() - t1;
            f.lastTo = toInclusive;
            f.lastRows = rows;
            logger.info(
                "mv_pull FANOUT_FOLLOWER leader=[{}] follower=[{}] range=({}, {}] rows={} fold_ms={} upload_ms={}",
                leaderIndex,
                f.index,
                fromExclusive,
                toInclusive,
                rows,
                nativeNanos / 1_000_000,
                uploadNanos / 1_000_000
            );
            if (f.push) {
                pushToFollower(f, shard.shardId().id(), sourceShardId, pub);
            }
            return new long[] { nativeNanos, uploadNanos, rows };
        } catch (Exception e) {
            f.broken = true;
            logger.error(
                "mv_pull FOLLOWER_BROKEN leader=[{}] follower=[{}] range=({}, {}] stop feeding: {}",
                leaderIndex,
                f.index,
                fromExclusive,
                toInclusive,
                e.getMessage(),
                e
            );
            return null;
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

    /**
     * D1: hand the publication to the follower primary over transport, asynchronously
     * (the leader's round never waits on a follower). A "gap" reply means the follower
     * is behind — resend everything after its applied watermark from the outbox chain.
     */
    private void pushToFollower(Follower f, int followerShardId, int sourceShardId, MVBuilderOutbox.Publication pub) {
        org.opensearch.transport.client.Client client = services.client();
        if (client == null) {
            logger.warn("mv_pull PUSH_SKIPPED follower=[{}] no client on this node; follower will poll", f.index);
            return;
        }
        String leaderIndex = indexSettings.getIndex().getName();
        long t0 = System.nanoTime();
        org.opensearch.mv.MVBuilderPublishAction.Request request = new org.opensearch.mv.MVBuilderPublishAction.Request(
            f.index,
            followerShardId,
            f.sourceIndexUuid,
            sourceShardId,
            pub
        );
        client.execute(org.opensearch.mv.MVBuilderPublishAction.INSTANCE, request, org.opensearch.core.action.ActionListener.wrap(resp -> {
            long rttMs = (System.nanoTime() - t0) / 1_000_000;
            logger.info(
                "mv_pull PUSH_ACK leader=[{}] follower=[{}] range=({}, {}] applied={} applied_wm={} publish_ms={} rtt_ms={} detail={}",
                leaderIndex,
                f.index,
                pub.fromExclusive(),
                pub.toInclusive(),
                resp.applied(),
                resp.appliedWatermark(),
                resp.publishMillis(),
                rttMs,
                resp.detail()
            );
            if (resp.applied() && resp.remoteSynced()) {
                services.threadPool().generic().execute(() -> trimOutbox(f, pub));
            }
            if (resp.applied() == false && resp.appliedWatermark() < pub.toInclusive()) {
                services.threadPool().generic().execute(() -> resyncFollower(f, followerShardId, sourceShardId, resp.appliedWatermark()));
            }
        },
            e -> logger.warn(
                "mv_pull PUSH_FAILED leader=[{}] follower=[{}] range=({}, {}] (follower will poll): {}",
                leaderIndex,
                f.index,
                pub.fromExclusive(),
                pub.toInclusive(),
                e.toString()
            )
        ));
    }

    /** The follower has published AND uploaded its own copy: the outbox copy is now redundant — delete it. */
    private void trimOutbox(Follower f, MVBuilderOutbox.Publication pub) {
        try {
            f.outbox.trim(pub);
            logger.info(
                "mv_pull OUTBOX_TRIM follower=[{}] to={} deleted=[{}, {}]",
                f.index,
                pub.toInclusive(),
                pub.stateBlob(),
                pub.manifestBlob()
            );
        } catch (Exception e) {
            logger.warn("mv_pull OUTBOX_TRIM_FAILED follower=[{}] to={}: {}", f.index, pub.toInclusive(), e.toString());
        }
    }

    /** Resend, in order and synchronously on the generic pool, every publication the follower has not applied yet. */
    private void resyncFollower(Follower f, int followerShardId, int sourceShardId, long appliedWatermark) {
        org.opensearch.transport.client.Client client = services.client();
        try {
            java.util.List<MVBuilderOutbox.Publication> missing = f.outbox.since(appliedWatermark);
            logger.info("mv_pull PUSH_RESYNC follower=[{}] applied_wm={} missing={}", f.index, appliedWatermark, missing.size());
            for (MVBuilderOutbox.Publication p : missing) {
                org.opensearch.mv.MVBuilderPublishAction.Response r = client.execute(
                    org.opensearch.mv.MVBuilderPublishAction.INSTANCE,
                    new org.opensearch.mv.MVBuilderPublishAction.Request(f.index, followerShardId, f.sourceIndexUuid, sourceShardId, p)
                ).actionGet(org.opensearch.common.unit.TimeValue.timeValueSeconds(120));
                if (r.applied() && r.remoteSynced()) {
                    trimOutbox(f, p);
                }
                if (r.applied() == false) {
                    logger.warn(
                        "mv_pull PUSH_RESYNC_STOPPED follower=[{}] at=({}, {}] detail={} applied_wm={}",
                        f.index,
                        p.fromExclusive(),
                        p.toInclusive(),
                        r.detail(),
                        r.appliedWatermark()
                    );
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn("mv_pull PUSH_RESYNC_FAILED follower=[{}] (follower will poll): {}", f.index, e.toString());
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
