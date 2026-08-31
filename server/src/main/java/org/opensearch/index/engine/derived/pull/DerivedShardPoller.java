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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicBoolean;
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
        // Check we're still a primary
        if (targetShard.routingEntry().primary() == false) {
            logger.info("derived_pull [{}] shard [{}] is no longer primary; closing poller", formatId, targetShard.shardId());
            close();
            return;
        }

        // Step 1: Fetch snapshot from remote source
        DerivedSourceSnapshot snapshot = reader.fetchSnapshot(targetShard.routingEntry(), currentWatermark);
        if (snapshot == null) {
            return; // No new data
        }
        if (snapshot.watermark() <= currentWatermark) {
            return; // Already at or beyond this watermark
        }

        // Step 2: Download to staging directory
        Path stageDir = workingDir.resolve("stage-" + snapshot.watermark());
        Files.createDirectories(stageDir);
        try {
            reader.downloadToStage(snapshot, stageDir);

            // Step 3: Build artifact
            BuildResult result = builder.build(snapshot, stageDir, targetShard);
            if (result.success()) {
                long previousWatermark = currentWatermark;
                currentWatermark = snapshot.watermark();
                logger.info(
                    "derived_pull [{}] shard [{}] published artifact={} watermark {} -> {} stats={}",
                    formatId,
                    targetShard.shardId(),
                    result.artifactId(),
                    previousWatermark,
                    currentWatermark,
                    result.stats()
                );
            } else {
                logger.warn(
                    "derived_pull [{}] shard [{}] build failed for watermark {}; will retry",
                    formatId,
                    targetShard.shardId(),
                    snapshot.watermark()
                );
            }
        } finally {
            cleanupStageDir(stageDir);
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
