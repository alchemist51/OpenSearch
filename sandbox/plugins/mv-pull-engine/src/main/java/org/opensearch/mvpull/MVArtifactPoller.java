/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVStateArtifactWriter;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Target-owned pull loop for a derived-only {@code mv_state} primary.
 *
 * <p>The poller discovers source snapshots, retains the existing observed-max
 * guard, builds one immutable range-state Arrow artifact, and delegates the
 * atomic catalog+watermark publication to {@link IndexShard}. Replicas never
 * instantiate this class.
 */
final class MVArtifactPoller implements Runnable, Closeable {

    private static final Logger logger = LogManager.getLogger(MVArtifactPoller.class);

    private final IndexShard targetShard;
    private final MVPullSettings.Services services;
    private final MVRemoteSource source;
    private final String groupField;
    private final String sumField;
    private final TimeValue interval;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Path workingPath;
    private final NIOFSDirectory workingDirectory;
    private final MVDataFusionReadEngine coverageReader;
    private final MVStateArtifactWriter artifactWriter = new MVStateArtifactWriter();

    private volatile MVWatermark watermark;

    MVArtifactPoller(IndexShard targetShard, MVPullSettings.Services services) throws IOException {
        this.targetShard = targetShard;
        this.services = services;
        var settings = targetShard.indexSettings().getSettings();
        String sourceIndexName = MVPullSettings.SOURCE_INDEX.get(settings);
        int sourceShardId = targetShard.shardId().id();
        this.source = new MVRemoteSource(services, sourceIndexName, sourceShardId);
        this.groupField = MVPullSettings.GROUP_FIELD.get(settings);
        this.sumField = MVPullSettings.SUM_FIELD.get(settings);
        this.interval = MVPullSettings.PULL_INTERVAL.get(settings);
        this.watermark = recoveredWatermark(targetShard, sourceShardId);
        this.workingPath = targetShard.shardPath().getDataPath().resolve("mv_pull_work");
        Files.createDirectories(workingPath);
        this.workingDirectory = new NIOFSDirectory(workingPath);
        this.coverageReader = new MVDataFusionReadEngine(workingPath);
        logger.info("mv_pull artifact poller starting shard={} from {}", targetShard.shardId(), watermark);
    }

    void start() {
        schedule(TimeValue.timeValueMillis(0));
    }

    private void schedule(TimeValue delay) {
        if (closed.get()) {
            return;
        }
        try {
            services.threadPool().schedule(this, delay, ThreadPool.Names.GENERIC);
        } catch (Exception e) {
            if (closed.get() == false) {
                logger.error("mv_pull failed to schedule artifact round", e);
            }
        }
    }

    @Override
    public void run() {
        if (closed.get()) {
            return;
        }
        try {
            round();
        } catch (Exception e) {
            if (closed.get() == false) {
                logger.error("mv_pull artifact round failed; watermark held at " + watermark, e);
            }
        } finally {
            schedule(interval);
        }
    }

    private void round() throws IOException {
        if (targetShard.routingEntry().primary() == false) {
            close();
            return;
        }

        MVWatermark current = watermark;
        MVRemoteSource.Advert advert = source.latestAdvert(workingDirectory);
        if (advert == null || advert.primaryTerm() < current.primaryTerm()) {
            return;
        }
        if (advert.maxSeqNo() <= current.seqNo()) {
            return;
        }

        List<Path> parquet = source.downloadedParquetFiles(workingPath);
        if (parquet.isEmpty()) {
            logger.warn(
                "mv_pull generation {} claims through {} without parquet; holding {}",
                advert.infosVersion(),
                advert.maxSeqNo(),
                current
            );
            return;
        }

        MVDataFusionReadEngine.Delta coverage = coverageReader.searchDelta(
            parquet,
            groupField,
            sumField,
            current.seqNo(),
            advert.maxSeqNo(),
            advert.infosVersion()
        );
        if (coverage.observedMaxSeqNo() < 0L) {
            return;
        }
        long appliedThrough = Math.min(coverage.observedMaxSeqNo(), advert.maxSeqNo());

        // ── Coverage integrity guard ──────────────────────────────────────
        // For an append-only contiguous source, the range (current.seqNo(),
        // appliedThrough] must contain exactly (appliedThrough - current.seqNo())
        // rows. Any mismatch means the parquet snapshot is incomplete or contains
        // duplicate sequence numbers. Advancing the watermark would make that
        // corruption permanent in the additive MV state.
        //
        // Hold the entire round until a later source generation exposes an exact,
        // duplicate-free range. Do not scan accumulated historical generations:
        // they can repeat sequence numbers and over-count the aggregate.
        long expectedRows = appliedThrough - current.seqNo();
        if (hasCompleteCoverage(current.seqNo(), appliedThrough, coverage.totalRows()) == false) {
            logger.warn(
                "mv_pull coverage mismatch: range=({}, {}] expected={} rows but found={}; "
                    + "holding watermark at {} (generation {} advert.maxSeqNo={}). "
                    + "The source must publish an exact, duplicate-free generation before this range can advance.",
                current.seqNo(),
                appliedThrough,
                expectedRows,
                coverage.totalRows(),
                current,
                advert.infosVersion(),
                advert.maxSeqNo()
            );
            return;
        }
        // ── End coverage integrity guard ──────────────────────────────────

        long generation = targetShard.reserveDerivedArtifactGeneration();
        Path stagedParquet = coverageReader.stageParquetFiles(parquet, generation);
        String filteredSql = partialSql(groupField, sumField, current.seqNo(), appliedThrough);
        MVStateArtifactWriter.Artifact artifact = artifactWriter.build(
            stagedParquet,
            MVConstants.INPUT_TABLE,
            filteredSql,
            targetShard.shardPath().getDataPath(),
            generation
        );
        MVWatermark next = new MVWatermark(advert.primaryTerm(), appliedThrough, advert.infosVersion());
        boolean published = false;
        try {
            targetShard.publishDerivedArtifact(
                MVStateDataFormat.INSTANCE,
                artifact.fileSet(),
                java.util.Map.of(MVWatermark.key(targetShard.shardId().id()), next.encode())
            );
            published = true;
            watermark = next;
            logger.info(
                "mv_pull published mv_state generation={} rows={} range=({}, {}] watermark={}",
                generation,
                artifact.stateRows(),
                current.seqNo(),
                appliedThrough,
                next
            );
        } finally {
            if (published == false && isArtifactReferenced(artifact.fileSet().writerGeneration()) == false) {
                Files.deleteIfExists(artifact.path());
            }
            coverageReader.cleanupStagedParquet(stagedParquet);
        }
    }

    /**
     * Returns whether an append-only source range contains every sequence number exactly once.
     * The expected cardinality of {@code (currentSeqNo, appliedThrough]} is their difference.
     */
    static boolean hasCompleteCoverage(long currentSeqNo, long appliedThrough, long totalRows) {
        return appliedThrough >= currentSeqNo && totalRows == appliedThrough - currentSeqNo;
    }

    private boolean isArtifactReferenced(long generation) {
        try (GatedCloseable<CatalogSnapshot> ref = targetShard.getCatalogSnapshot()) {
            return ref.get().getSegments().stream().anyMatch(segment -> segment.generation() == generation);
        } catch (Exception e) {
            logger.warn("mv_pull could not verify artifact reference for generation [" + generation + "]; retaining file", e);
            return true;
        }
    }

    private static String partialSql(String groupField, String sumField, long fromExclusive, long toInclusive) {
        return String.format(
            Locale.ROOT,
            "SELECT \"%s\", COUNT(*), SUM(\"%s\") "
                + "FROM (SELECT * FROM %s WHERE \"_seq_no\" > %d AND \"_seq_no\" <= %d) AS %s "
                + "GROUP BY \"%s\"",
            groupField,
            sumField,
            MVConstants.INPUT_TABLE,
            fromExclusive,
            toInclusive,
            MVConstants.INPUT_TABLE,
            groupField
        );
    }

    private static MVWatermark recoveredWatermark(IndexShard shard, int sourceShardId) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = shard.getCatalogSnapshot()) {
            String encoded = ref.get().getUserData().get(MVWatermark.key(sourceShardId));
            return encoded == null ? MVWatermark.EMPTY : MVWatermark.decode(encoded);
        }
    }

    MVWatermark watermark() {
        return watermark;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            coverageReader.close();
            workingDirectory.close();
        }
    }
}
