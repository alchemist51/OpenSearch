/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.engine.derived.pull.spi.PollRoundStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Generic per-shard poll loop for derived data formats.
 *
 * <p>Executes a repeating cycle: {@code fetchSnapshot → downloadToStage →
 * build → advance watermark}. All format-specific work is delegated to
 * {@link DerivedSourceReader} and {@link DerivedArtifactBuilder} instances
 * obtained from the registered {@link DerivedPullFormat}.
 *
 * <p>This class is intentionally format-agnostic: it does not import or
 * reference any MV, DataFusion, Parquet, SegmentInfos, or other format-
 * specific types.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DerivedShardPoller implements Runnable, Closeable {

    private static final Logger logger = LogManager.getLogger(DerivedShardPoller.class);

    private final IndexShard targetShard;
    private final DerivedSourceReader reader;
    private final DerivedArtifactBuilder builder;
    private final String formatId;
    private final TimeValue interval;
    private final ThreadPool threadPool;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Path workingDir;

    private volatile long currentWatermark;

    // ── Cumulative stage metrics (thread-safe) ──────────────────────────
    private final AtomicLong roundCount = new AtomicLong();
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();
    private final Map<String, StageAccumulator> cumulativeStages = new HashMap<>();
    /** Per-round samples, bounded to the last 256 rounds. */
    private final ConcurrentLinkedDeque<PollRoundStats> roundSamples = new ConcurrentLinkedDeque<>();
    private static final int MAX_SAMPLES = 256;

    /**
     * Creates a new poller for the given target shard.
     *
     * @param targetShard the locally-started primary shard to pull into
     * @param format      the format-specific SPI implementation
     * @param interval    polling interval
     * @param threadPool  for scheduling
     * @param initialWatermark the recovered watermark from the shard's commit
     *                         user data (-1 for "never pulled")
     */
    public DerivedShardPoller(
        IndexShard targetShard,
        DerivedPullFormat format,
        TimeValue interval,
        ThreadPool threadPool,
        long initialWatermark
    ) {
        this.targetShard = targetShard;
        this.formatId = format.formatId();
        this.reader = format.createReader(targetShard.indexSettings().getNodeSettings(), targetShard.indexSettings());
        this.builder = format.createArtifactBuilder(targetShard.indexSettings().getNodeSettings(), targetShard.indexSettings());
        this.interval = interval;
        this.threadPool = threadPool;
        this.currentWatermark = initialWatermark;
        this.workingDir = targetShard.shardPath().getDataPath().resolve("derived_pull_work");
        try {
            Files.createDirectories(workingDir);
        } catch (IOException e) {
            throw new IllegalStateException("derived_pull: failed to create working directory " + workingDir, e);
        }
        logger.info("derived_pull [{}] poller created for shard [{}] watermark={}", formatId, targetShard.shardId(), initialWatermark);
    }

    /** Start the first poll round (zero-delay). */
    public void start() {
        schedule(TimeValue.timeValueMillis(0));
    }

    private void schedule(TimeValue delay) {
        if (closed.get()) {
            return;
        }
        try {
            threadPool.schedule(this, delay, ThreadPool.Names.GENERIC);
        } catch (Exception e) {
            if (closed.get() == false) {
                logger.error("derived_pull [{}] failed to schedule poll round for shard [{}]", formatId, targetShard.shardId(), e);
            }
        }
    }

    @Override
    public void run() {
        if (closed.get()) {
            return;
        }
        try {
            pollRound();
        } catch (Exception e) {
            if (closed.get() == false) {
                logger.error(
                    "derived_pull [{}] poll round failed for shard [{}]; watermark held at {}",
                    formatId,
                    targetShard.shardId(),
                    currentWatermark,
                    e
                );
            }
        } finally {
            schedule(interval);
        }
    }

    private void pollRound() throws IOException {
        roundCount.incrementAndGet();
        PollRoundStats.Builder statsBuilder = new PollRoundStats.Builder().startRound();

        // Check we're still a primary
        if (targetShard.routingEntry().primary() == false) {
            logger.info("derived_pull [{}] shard [{}] is no longer primary; closing poller", formatId, targetShard.shardId());
            close();
            return;
        }

        // Step 1: Fetch snapshot from remote source
        long t0 = System.nanoTime();
        DerivedSourceSnapshot snapshot = reader.fetchSnapshot(targetShard.routingEntry(), currentWatermark);
        statsBuilder.stage("fetch_snapshot", System.nanoTime() - t0);
        if (snapshot == null) {
            return; // No new data
        }
        if (snapshot.watermark() <= currentWatermark) {
            return; // Already at or beyond this watermark
        }

        // Step 2: Download to staging directory
        Path stageDir = workingDir.resolve("stage-" + snapshot.watermark());
        Files.createDirectories(stageDir);
        BuildResult result = null;
        try {
            long t1 = System.nanoTime();
            reader.downloadToStage(snapshot, stageDir);
            statsBuilder.stage("download", System.nanoTime() - t1);

            // Step 3: Build artifact
            long t2 = System.nanoTime();
            result = builder.build(snapshot, stageDir, targetShard);
            statsBuilder.stage("build", System.nanoTime() - t2);

            // Merge build-specific sub-stage stats: entries whose key ends
            // with "_nanos" are treated as stage durations and get full
            // cumulative count/total/mean/max tracking via StageAccumulator;
            // everything else is recorded as a simple counter.
            if (result != null) {
                result.stats().forEach((k, v) -> {
                    if (v instanceof Number) {
                        if (k.endsWith("_nanos")) {
                            statsBuilder.stage(k, ((Number) v).longValue());
                        } else {
                            statsBuilder.counter(k, ((Number) v).longValue());
                        }
                    }
                });
            }
        } finally {
            long t3 = System.nanoTime();
            cleanupStageDir(stageDir);
            statsBuilder.stage("cleanup", System.nanoTime() - t3);
        }

        if (result != null && result.success()) {
            successCount.incrementAndGet();
            long previousWatermark = currentWatermark;
            currentWatermark = snapshot.watermark();
            statsBuilder.counter("source_watermark", snapshot.watermark());
            statsBuilder.counter("target_watermark", currentWatermark);
            statsBuilder.counter("lag", Math.max(0L, snapshot.watermark() - currentWatermark));
            statsBuilder.counter("round_success", 1L);

            // Finalize only after outcome/watermark counters are recorded.
            PollRoundStats roundStats = statsBuilder.build();
            recordRoundStats(roundStats);
            logger.info(
                "derived_pull [{}] shard [{}] published artifact={} watermark {} -> {} stats={}",
                formatId,
                targetShard.shardId(),
                result.artifactId(),
                previousWatermark,
                currentWatermark,
                roundStats
            );
        } else {
            failureCount.incrementAndGet();
            statsBuilder.counter("source_watermark", snapshot.watermark());
            statsBuilder.counter("target_watermark", currentWatermark);
            statsBuilder.counter("lag", Math.max(0L, snapshot.watermark() - currentWatermark));
            statsBuilder.counter("round_success", 0L);

            PollRoundStats roundStats = statsBuilder.build();
            recordRoundStats(roundStats);
            logger.warn(
                "derived_pull [{}] shard [{}] build failed for watermark {}; will retry. stats={}",
                formatId,
                targetShard.shardId(),
                snapshot.watermark(),
                roundStats
            );
        }
    }

    private void recordRoundStats(PollRoundStats stats) {
        roundSamples.addLast(stats);
        while (roundSamples.size() > MAX_SAMPLES) {
            roundSamples.pollFirst();
        }
        // Accumulate each named stage plus the total round duration.
        synchronized (cumulativeStages) {
            stats.stageNanos().forEach((stage, nanos) -> cumulativeStages.computeIfAbsent(stage, k -> new StageAccumulator()).add(nanos));
            cumulativeStages.computeIfAbsent("total_round", k -> new StageAccumulator()).add(stats.totalNanos());
        }
    }

    private void cleanupStageDir(Path stageDir) {
        try {
            if (Files.exists(stageDir)) {
                try (Stream<Path> walk = Files.walk(stageDir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            logger.warn("derived_pull failed to delete staged file " + p, e);
                        }
                    });
                }
            }
        } catch (IOException e) {
            logger.warn("derived_pull failed to clean staging directory " + stageDir, e);
        }
    }

    /** Returns the current watermark. */
    public long watermark() {
        return currentWatermark;
    }

    /** Returns the format identifier this poller is pulling for. */
    public String formatId() {
        return formatId;
    }

    /** Returns whether this poller has been closed. */
    public boolean isClosed() {
        return closed.get();
    }

    /** Returns the total number of poll rounds attempted. */
    public long roundCount() {
        return roundCount.get();
    }

    /** Returns the number of successful build rounds. */
    public long successCount() {
        return successCount.get();
    }

    /** Returns the number of failed build rounds. */
    public long failureCount() {
        return failureCount.get();
    }

    /** Returns cumulative stage metrics (count/total/mean/max per stage). */
    public Map<String, StageAccumulator> cumulativeStages() {
        synchronized (cumulativeStages) {
            return new HashMap<>(cumulativeStages);
        }
    }

    /** Returns the per-round samples (most recent up to 256 rounds). */
    public java.util.List<PollRoundStats> roundSamples() {
        return java.util.List.copyOf(roundSamples);
    }

    /**
     * Cumulative stage timing accumulator: count, total nanos, max nanos.
     * Mean is computed as total/count. Suitable for offline p50/p90/p95/p99
     * when combined with per-round samples.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class StageAccumulator {
        private long count;
        private long totalNanos;
        private long maxNanos;

        void add(long nanos) {
            count++;
            totalNanos += nanos;
            maxNanos = Math.max(maxNanos, nanos);
        }

        public long count() {
            return count;
        }

        public long totalNanos() {
            return totalNanos;
        }

        public long maxNanos() {
            return maxNanos;
        }

        public long meanNanos() {
            return count > 0 ? totalNanos / count : 0;
        }

        @Override
        public String toString() {
            return "count="
                + count
                + " total="
                + (totalNanos / 1_000_000)
                + "ms mean="
                + (meanNanos() / 1_000_000)
                + "ms max="
                + (maxNanos / 1_000_000)
                + "ms";
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            try {
                reader.close();
            } catch (IOException e) {
                logger.warn("derived_pull [{}] shard [{}] failed to close reader", formatId, targetShard.shardId(), e);
            }
            try {
                builder.close();
            } catch (IOException e) {
                logger.warn("derived_pull [{}] shard [{}] failed to close builder", formatId, targetShard.shardId(), e);
            }
            logger.info("derived_pull [{}] poller closed for shard [{}]", formatId, targetShard.shardId());
        }
    }
}
