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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.mv.MVBuilderPublishAction;
import org.opensearch.mv.MVConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Builder-shard emulation, follower side. Performs no fold: a publication
 * already folded by the leader is moved into this shard's {@code mv_state}
 * directory and published through {@link IndexShard#publishDerivedArtifact}
 * exactly like a locally built generation — same catalog, same compaction,
 * same remote upload of the follower's own copy.
 *
 * <p>Two ways in, one publish path:
 * <ul>
 *   <li><b>push</b> (D1, default): the leader's {@link MVBuilderPublishAction} request arrives at the
 *       follower primary and {@link #applyPushed} downloads the named state file and publishes it;</li>
 *   <li><b>poll</b> (recovery floor, or {@code hydrate_transport=poll}): the {@link MVBuilderOutboxReader}
 *       stages whatever the outbox chain holds beyond the watermark and {@link #build} publishes it.</li>
 * </ul>
 * Both take the per-shard lock and are idempotent on the applied watermark, so
 * a publication delivered by both paths is published once.
 *
 * <p>Emits the same {@code mv_pull published generation=…} line as the
 * direct-write builder so the existing lag tooling applies unchanged, plus
 * {@code hop_ms}: follower publish time minus the leader's outbox publish
 * time (wall clocks of two nodes; NTP-bounded).
 */
public final class MVHydrateArtifactBuilder implements DerivedArtifactBuilder {

    private static final Logger logger = LogManager.getLogger(MVHydrateArtifactBuilder.class);

    /** Last applied source seq-no per follower shard (-1 = nothing applied); recovered from commit user data on first use. */
    private static final ConcurrentHashMap<ShardId, Long> APPLIED = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<ShardId, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    static long appliedWatermark(ShardId shardId) {
        return APPLIED.getOrDefault(shardId, -1L);
    }

    static ReentrantLock lockFor(ShardId shardId) {
        return LOCKS.computeIfAbsent(shardId, k -> new ReentrantLock());
    }

    /** Recover the applied watermark from the shard's commit user data once per shard lifetime. */
    static long ensureRecovered(IndexShard shard, int sourceShardId) throws IOException {
        Long applied = APPLIED.get(shard.shardId());
        if (applied == null) {
            applied = MVDerivedArtifactBuilder.recoveredWatermark(shard, sourceShardId).seqNo();
            APPLIED.put(shard.shardId(), applied);
        }
        return applied;
    }

    /** Outcome of offering one publication to a shard. */
    enum Outcome {
        PUBLISHED,
        ALREADY_APPLIED,
        GAP
    }

    record Applied(Outcome outcome, long watermark, long publishNanos, long hopMs, long generation) {
    }

    /**
     * Publish one staged state file as the next generation. Caller holds
     * {@link #lockFor(ShardId)}. Never skips a range: a publication that does
     * not continue exactly at the applied watermark is reported as {@link Outcome#GAP}.
     */
    static Applied publishStaged(IndexShard shard, int sourceShardId, MVBuilderOutbox.Publication pub, Path staged, String via)
        throws IOException {
        long applied = ensureRecovered(shard, sourceShardId);
        if (pub.toInclusive() <= applied) {
            return new Applied(Outcome.ALREADY_APPLIED, applied, 0L, -1L, -1L);
        }
        if (applied >= 0 && pub.fromExclusive() != applied) {
            logger.error(
                "mv_pull HYDRATE_GAP target=[{}] applied_through={} offered=({}, {}] via={} — refusing to skip",
                shard.shardId(),
                applied,
                pub.fromExclusive(),
                pub.toInclusive(),
                via
            );
            return new Applied(Outcome.GAP, applied, 0L, -1L, -1L);
        }

        Path formatDirectory = shard.shardPath().getDataPath().resolve(MVConstants.STATE_ARTIFACT_FORMAT);
        Files.createDirectories(formatDirectory);
        long generation = shard.reserveDerivedArtifactGeneration();
        String fileName = MVConstants.stateFileName(generation);
        Path completed = formatDirectory.resolve(fileName);
        MVDerivedArtifactBuilder.moveCompletedArtifact(staged, completed);

        WriterFileSet fileSet = MonoFileWriterSet.of(formatDirectory.toAbsolutePath(), generation, fileName, pub.rows());
        MVWatermark next = new MVWatermark(pub.primaryTerm(), pub.toInclusive(), pub.infosVersion());

        long tPublish = System.nanoTime();
        shard.publishDerivedArtifact(MVConstants.STATE_ARTIFACT_FORMAT, fileSet, Map.of(MVWatermark.key(sourceShardId), next.encode()));
        long publishNanos = System.nanoTime() - tPublish;
        APPLIED.put(shard.shardId(), next.seqNo());
        MVStateChecksumUtil.computeAndRegister(completed, fileName, generation, shard);

        long hopMs = System.currentTimeMillis() - pub.publishedEpochMs();
        // Same shape as the direct-write line (parsed by the lag tooling) + hop_ms.
        logger.info(
            "mv_pull published generation={} rows={} range=({}, {}] watermark={} "
                + "coverage=0ms native_build=0ms publish={}ms schema_hash=- capped=false remaining_lag=0 hop_ms={} leader=[{}] [{}]",
            generation,
            pub.rows(),
            pub.fromExclusive(),
            pub.toInclusive(),
            next,
            publishNanos / 1_000_000,
            hopMs,
            pub.leaderIndex(),
            via
        );
        return new Applied(Outcome.PUBLISHED, next.seqNo(), publishNanos, hopMs, generation);
    }

    /**
     * Push path (D1): download the publication's state file from this
     * follower's outbox and publish it. Runs on the follower primary's node
     * inside the transport handler.
     */
    public static MVBuilderPublishAction.Response applyPushed(
        IndexShard shard,
        MVPullSettings.Services services,
        MVBuilderPublishAction.Request request
    ) throws IOException {
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(shard.indexSettings().getSettings());
        if (binding == null || binding.sourceName() == null) {
            throw new IllegalStateException("mv_pull hydrate: target [" + shard.shardId().getIndexName() + "] has no source binding");
        }
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        if (sourceShardId != request.sourceShard()) {
            throw new IllegalArgumentException(
                "mv_pull hydrate: publication is for source shard " + request.sourceShard() + " but this target follows " + sourceShardId
            );
        }
        MVBuilderOutbox.Publication pub = request.publication();
        ReentrantLock lock = lockFor(shard.shardId());
        lock.lock();
        try {
            long applied = ensureRecovered(shard, sourceShardId);
            if (pub.toInclusive() <= applied) {
                return new MVBuilderPublishAction.Response(true, applied, 0L, "already-applied");
            }
            if (applied >= 0 && pub.fromExclusive() != applied) {
                return new MVBuilderPublishAction.Response(false, applied, 0L, "gap");
            }
            IndexMetadata source = services.sourceIndexMetadata(binding.sourceName());
            if (source.getIndexUUID().equals(request.sourceIndexUuid()) == false) {
                return new MVBuilderPublishAction.Response(false, applied, 0L, "source-uuid-mismatch");
            }
            String repository = source.getSettings().get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
            MVBuilderOutbox outbox = MVBuilderOutbox.open(
                services.repositoriesService(),
                repository,
                request.sourceIndexUuid(),
                sourceShardId,
                shard.shardId().getIndexName()
            );
            Path work = shard.shardPath().getDataPath().resolve("derived_pull_work");
            Files.createDirectories(work);
            Path staged = work.resolve("push-" + pub.stateBlob());
            long t0 = System.nanoTime();
            outbox.download(pub, staged);
            long downloadMs = (System.nanoTime() - t0) / 1_000_000;
            Applied result = publishStaged(shard, sourceShardId, pub, staged, "hydrate-push download_ms=" + downloadMs);
            return new MVBuilderPublishAction.Response(
                result.outcome() != Outcome.GAP,
                result.watermark(),
                result.publishNanos() / 1_000_000,
                result.outcome().name().toLowerCase(java.util.Locale.ROOT)
            );
        } finally {
            lock.unlock();
        }
    }

    private final IndexSettings indexSettings;
    private final DerivedIndexBinding binding;

    MVHydrateArtifactBuilder(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        if (binding == null || binding.sourceName() == null) {
            throw new IllegalStateException("mv_pull hydrate: target [" + indexSettings.getIndex().getName() + "] has no source binding");
        }
    }

    /** Poll path: publish every staged publication in order (recovery floor under push, the hot path under poll). */
    @Override
    public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
        MVBuilderOutboxReader.HydrateSnapshot hs = (MVBuilderOutboxReader.HydrateSnapshot) snapshot;
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());

        long publishNanosTotal = 0L;
        long rowsTotal = 0L;
        long lastHopMs = -1L;
        long lastGeneration = -1L;
        int published = 0;
        int skipped = 0;

        ReentrantLock lock = lockFor(shard.shardId());
        for (MVBuilderOutbox.Publication pub : hs.publications()) {
            Path staged = stageDir.resolve(pub.stateBlob());
            Applied a;
            lock.lock();
            try {
                a = publishStaged(shard, sourceShardId, pub, staged, "hydrate");
            } finally {
                lock.unlock();
            }
            switch (a.outcome()) {
                case ALREADY_APPLIED -> skipped++;
                case GAP -> {
                    return new MVDerivedArtifactBuilder.MVBuildResult(false, "hydrate-gap", Map.of("applied_watermark", a.watermark()));
                }
                case PUBLISHED -> {
                    publishNanosTotal += a.publishNanos();
                    rowsTotal += pub.rows();
                    lastHopMs = a.hopMs();
                    lastGeneration = a.generation();
                    published++;
                }
            }
        }

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("hydrate_skipped", (long) skipped);
        stats.put("capped", false);
        if (published == 0) {
            // Everything in the snapshot was already applied (typically by the push path): success, no new artifact.
            return new MVDerivedArtifactBuilder.MVBuildResult(true, "noop", stats);
        }
        stats.put("stateRows", rowsTotal);
        stats.put("generation", lastGeneration);
        stats.put("publish_nanos", publishNanosTotal);
        stats.put("hydrate_publications", (long) published);
        stats.put("hop_ms", lastHopMs);
        stats.put("hydrate", true);
        return new MVDerivedArtifactBuilder.MVBuildResult(true, "gen-" + lastGeneration, stats);
    }

    @Override
    public void close() {}
}
