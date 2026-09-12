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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.mv.MVConstants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Builder-shard emulation, follower side: a {@link DerivedArtifactBuilder}
 * that performs no fold. Each staged publication (already folded by the
 * leader) is moved into this shard's {@code mv_state} directory and published
 * through {@link IndexShard#publishDerivedArtifact} exactly like a locally
 * built generation — same catalog, same compaction, same remote upload of the
 * follower's own copy.
 *
 * <p>Emits the same {@code mv_pull published generation=…} line as the
 * direct-write builder so the existing lag tooling applies unchanged, plus
 * {@code hop_ms}: follower publish time minus the leader's outbox publish
 * time (wall clocks of two nodes; NTP-bounded).
 */
final class MVHydrateArtifactBuilder implements DerivedArtifactBuilder {

    private static final Logger logger = LogManager.getLogger(MVHydrateArtifactBuilder.class);

    /** Last applied source seq-no per follower shard, republished for the reader after watermark recovery. */
    private static final ConcurrentHashMap<ShardId, Long> APPLIED = new ConcurrentHashMap<>();

    static long appliedWatermark(ShardId shardId) {
        return APPLIED.getOrDefault(shardId, -1L);
    }

    private final IndexSettings indexSettings;
    private final DerivedIndexBinding binding;
    private volatile MVWatermark watermark;

    MVHydrateArtifactBuilder(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        if (binding == null || binding.sourceName() == null) {
            throw new IllegalStateException("mv_pull hydrate: target [" + indexSettings.getIndex().getName() + "] has no source binding");
        }
    }

    @Override
    public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException {
        MVBuilderOutboxReader.HydrateSnapshot hs = (MVBuilderOutboxReader.HydrateSnapshot) snapshot;
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        if (watermark == null) {
            watermark = MVDerivedArtifactBuilder.recoveredWatermark(shard, sourceShardId);
            APPLIED.put(shard.shardId(), watermark.seqNo());
        }

        Path formatDirectory = shard.shardPath().getDataPath().resolve(MVConstants.STATE_ARTIFACT_FORMAT);
        Files.createDirectories(formatDirectory);

        long publishNanosTotal = 0L;
        long rowsTotal = 0L;
        long lastHopMs = -1L;
        long lastGeneration = -1L;
        int published = 0;
        int skipped = 0;

        for (MVBuilderOutbox.Publication pub : hs.publications()) {
            if (pub.toInclusive() <= watermark.seqNo()) {
                skipped++; // already applied before a restart; the reader re-walked past it
                continue;
            }
            if (pub.fromExclusive() != watermark.seqNo() && watermark.seqNo() >= 0) {
                // The chain must continue exactly where this follower stopped; anything else is a gap.
                logger.error(
                    "mv_pull HYDRATE_GAP target=[{}] applied_through={} next_publication=({}, {}] — refusing to skip",
                    shard.shardId(),
                    watermark.seqNo(),
                    pub.fromExclusive(),
                    pub.toInclusive()
                );
                return new MVDerivedArtifactBuilder.MVBuildResult(false, "hydrate-gap", Map.of());
            }
            Path staged = stageDir.resolve(pub.stateBlob());
            long generation = shard.reserveDerivedArtifactGeneration();
            String fileName = MVConstants.stateFileName(generation);
            Path completed = formatDirectory.resolve(fileName);
            MVDerivedArtifactBuilder.moveCompletedArtifact(staged, completed);

            WriterFileSet fileSet = MonoFileWriterSet.of(formatDirectory.toAbsolutePath(), generation, fileName, pub.rows());
            MVWatermark next = new MVWatermark(pub.primaryTerm(), pub.toInclusive(), pub.infosVersion());

            long tPublish = System.nanoTime();
            shard.publishDerivedArtifact(MVConstants.STATE_ARTIFACT_FORMAT, fileSet, Map.of(MVWatermark.key(sourceShardId), next.encode()));
            long publishNanos = System.nanoTime() - tPublish;
            watermark = next;
            APPLIED.put(shard.shardId(), next.seqNo());
            MVStateChecksumUtil.computeAndRegister(completed, fileName, generation, shard);

            long hopMs = System.currentTimeMillis() - pub.publishedEpochMs();
            publishNanosTotal += publishNanos;
            rowsTotal += pub.rows();
            lastHopMs = hopMs;
            lastGeneration = generation;
            published++;

            // Same shape as the direct-write line (parsed by the lag tooling) + hop_ms.
            logger.info(
                "mv_pull published generation={} rows={} range=({}, {}] watermark={} "
                    + "coverage=0ms native_build=0ms publish={}ms schema_hash=- capped=false remaining_lag=0 hop_ms={} leader=[{}] [hydrate]",
                generation,
                pub.rows(),
                pub.fromExclusive(),
                pub.toInclusive(),
                next,
                publishNanos / 1_000_000,
                hopMs,
                pub.leaderIndex()
            );
        }

        if (published == 0) {
            // Everything in the snapshot was already applied: report success without a new artifact.
            Map<String, Object> stats = new LinkedHashMap<>();
            stats.put("hydrate_skipped", (long) skipped);
            stats.put("capped", false);
            return new MVDerivedArtifactBuilder.MVBuildResult(true, "noop", stats);
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("stateRows", rowsTotal);
        stats.put("generation", lastGeneration);
        stats.put("publish_nanos", publishNanosTotal);
        stats.put("hydrate_publications", (long) published);
        stats.put("hydrate_skipped", (long) skipped);
        stats.put("hop_ms", lastHopMs);
        stats.put("hydrate", true);
        stats.put("capped", false);
        return new MVDerivedArtifactBuilder.MVBuildResult(true, "gen-" + lastGeneration, stats);
    }

    @Override
    public void close() {}
}
