/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Target-side hydrator: polls source shard(s) for new MV generations via
 * transport {@code GetMVCheckpoint}, then downloads data files directly from
 * remote store (RemoteStoreReplicationSource idiom — no source transport for files).
 *
 * <p>Runs on target primary shards whose IndexMetadata customData has {@code mv_binding}.
 * Thin poll loop on the GENERIC thread pool with configurable interval.</p>
 *
 * <p>Ingest-independent: never blocks anything. Failures log + retry next tick.</p>
 *
 * @opensearch.internal
 */
public class MVTargetHydrator implements Closeable {

    private static final Logger logger = LogManager.getLogger(MVTargetHydrator.class);

    /** POC polling interval (target-side). */
    public static final Setting<TimeValue> HYDRATE_INTERVAL = Setting.timeSetting(
        "index.mv.hydrate_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(100),
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** File count threshold that triggers compaction for a source-shard directory. */
    public static final Setting<Integer> COMPACT_THRESHOLD = Setting.intSetting(
        "index.mv.compact_threshold",
        8,
        2,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** Pattern for compacted file names: _mv_compacted.s{shard}.g{minGen}-{maxGen}.{uuid}.parquet */
    private static final Pattern COMPACTED_NAME_PATTERN =
        Pattern.compile("_mv_compacted\\.s(\\d+)\\.g(\\d+)-(\\d+)\\.[^.]+\\.parquet");

    /**
     * The artifact format name used when publishing hydrated MV state files into the
     * target engine's catalog. Must match the derived target artifact name that the
     * analytics-engine's ShardScanInstructionHandler dispatches on (i.e.
     * {@code registry.derivedTargetArtifact(category).name()} where category =
     * "materialized_view"). The pull-POC uses "mv_state" (MVStateDataFormat.NAME).
     */
    static final String MV_STATE_FORMAT_NAME = "mv_state";

    private final ShardId targetShardId;
    private final String mvId;
    private final String sourceIndex;
    private final Path hydratedDir;
    private final TransportService transportService;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final MVStateRemoteManager remoteManager;
    private final TimeValue interval;

    /** Per source-shard high-water: (term, generation). */
    private final ConcurrentHashMap<Integer, HighWater> highWaters = new ConcurrentHashMap<>();

    /** Single-flight guard: source shards currently being compacted. */
    private final Set<Integer> compactingShards = ConcurrentHashMap.newKeySet();

    private volatile int compactThreshold;

    private volatile org.opensearch.threadpool.Scheduler.Cancellable scheduledFuture;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MVTargetHydrator(
        ShardId targetShardId,
        String mvId,
        String sourceIndex,
        Path targetShardDataPath,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MVStateRemoteManager remoteManager,
        TimeValue interval
    ) {
        this.targetShardId = targetShardId;
        this.mvId = mvId;
        this.sourceIndex = sourceIndex;
        this.hydratedDir = targetShardDataPath.resolve("mv_hydrated");
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.remoteManager = remoteManager;
        this.interval = interval;
        this.compactThreshold = 8; // default; updated by setting consumer
    }

    /**
     * Start the poll loop. Idempotent.
     */
    public void start() {
        if (closed.get()) {
            return;
        }
        // Rebuild high-water from existing hydrated files
        rebuildHighWater();
        scheduledFuture = threadPool.scheduleWithFixedDelay(this::pollOnce, interval, ThreadPool.Names.GENERIC);
        logger.info(
            "MV hydrator started: target={} mvId={} source={} interval={}",
            targetShardId, mvId, sourceIndex, interval
        );
    }

    /**
     * Stop the poll loop. Idempotent.
     */
    public void stop() {
        if (closed.compareAndSet(false, true)) {
            org.opensearch.threadpool.Scheduler.Cancellable f = scheduledFuture;
            if (f != null) {
                f.cancel();
            }
            logger.info("MV hydrator stopped: target={} mvId={}", targetShardId, mvId);
        }
    }

    @Override
    public void close() {
        stop();
    }

    /**
     * One poll tick: for each source shard, fetch checkpoint and download new generations.
     * After hydration, check if any source-shard directory needs compaction.
     */
    void pollOnce() {
        if (closed.get()) {
            return;
        }
        try {
            ClusterState state = clusterService.state();
            IndexMetadata sourceMetadata = state.metadata().index(sourceIndex);
            if (sourceMetadata == null) {
                logger.debug("MV hydrator: source index {} not found in cluster state", sourceIndex);
                return;
            }
            int numShards = sourceMetadata.getNumberOfShards();
            for (int sourceShard = 0; sourceShard < numShards; sourceShard++) {
                try {
                    pollSourceShard(state, sourceMetadata, sourceShard);
                } catch (Exception e) {
                    logger.warn(
                        "MV hydrator poll failed: target={} source=[{}][{}] error={}",
                        targetShardId, sourceIndex, sourceShard, e.getMessage()
                    );
                }
            }
            // After hydration: check compaction eligibility for all source-shard dirs.
            tryCompactAll();
        } catch (Exception e) {
            logger.warn("MV hydrator poll cycle failed: target={} error={}", targetShardId, e.getMessage());
        }
    }

    /**
     * Check all source-shard hydrated directories for compaction eligibility.
     */
    private void tryCompactAll() {
        // The production callback is installed by the MV engine once definition-aware
        // compaction is wired. Until then, avoid scanning/logging every hydration tick.
        if (compactCallback == null) {
            return;
        }
        if (!Files.isDirectory(hydratedDir)) {
            return;
        }
        try (var shardDirs = Files.newDirectoryStream(hydratedDir)) {
            for (Path shardDir : shardDirs) {
                if (!Files.isDirectory(shardDir)) continue;
                int sourceShard;
                try {
                    sourceShard = Integer.parseInt(shardDir.getFileName().toString());
                } catch (NumberFormatException e) {
                    continue;
                }
                tryCompactSourceShard(sourceShard, shardDir);
            }
        } catch (IOException e) {
            logger.debug("MV hydrator compact scan failed: {}", e.getMessage());
        }
    }

    /**
     * Try to compact a single source-shard directory if eligible.
     * Eligibility: file count >= threshold AND no compaction already running for this shard.
     */
    private void tryCompactSourceShard(int sourceShard, Path shardDir) {
        // Single-flight guard: skip if already compacting this shard.
        if (!compactingShards.add(sourceShard)) {
            return;
        }
        try {
            // Snapshot the CURRENT file list. Files hydrated during compaction will survive.
            List<Path> snapshot = new ArrayList<>();
            try (var files = Files.newDirectoryStream(shardDir, "*.parquet")) {
                for (Path f : files) {
                    snapshot.add(f);
                }
            }

            if (snapshot.size() < compactThreshold) {
                return; // not enough files
            }

            // Extract gen range from filenames for the output name.
            long minGen = Long.MAX_VALUE;
            long maxGen = Long.MIN_VALUE;
            List<String> inputPaths = new ArrayList<>();
            for (Path f : snapshot) {
                String name = f.getFileName().toString();
                long gen = parseGenFromFileName(name);
                // Also check compacted gen range.
                long[] range = parseCompactedGenRange(name);
                if (range != null) {
                    if (range[0] < minGen) minGen = range[0];
                    if (range[1] > maxGen) maxGen = range[1];
                } else if (gen > 0) {
                    if (gen < minGen) minGen = gen;
                    if (gen > maxGen) maxGen = gen;
                }
                inputPaths.add(f.toAbsolutePath().toString());
            }
            if (minGen == Long.MAX_VALUE) minGen = 0;
            if (maxGen == Long.MIN_VALUE) maxGen = 0;

            // Output: tmp file, then atomic rename.
            String uuid = UUID.randomUUID().toString().substring(0, 8);
            String tmpName = "_mv_compacted." + uuid + ".parquet.tmp";
            String finalName = "_mv_compacted.s" + sourceShard + ".g" + minGen + "-" + maxGen + "." + uuid + ".parquet";
            Path tmpPath = shardDir.resolve(tmpName);
            Path finalPath = shardDir.resolve(finalName);

            logger.info(
                "MV compact starting: target={} shard={} files={} genRange={}-{}",
                targetShardId, sourceShard, snapshot.size(), minGen, maxGen
            );

            try {
                // Invoke native compaction. For POC, we don't have the full
                // definition SQL and schema available here (they're stored in
                // IndexMetadata). This is wired via a compact callback interface
                // in the production path. For POC, the compaction is a sorted
                // k-way merge (no fold) which only needs the sort keys.
                // The actual native call is done via MVNativeBridge.compact() in
                // the mv-engine plugin. Since server module can't depend on mv-engine,
                // we use a callback pattern.
                if (compactCallback != null) {
                    compactCallback.compact(inputPaths, tmpPath.toAbsolutePath().toString());
                } else {
                    logger.debug("MV compact: no callback registered, skipping native compact");
                    return;
                }

                // Atomic finish: rename tmp -> final.
                Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE);

                // Delete EXACTLY the snapshot input files (files hydrated during compaction survive).
                int deleted = 0;
                for (Path f : snapshot) {
                    try {
                        Files.deleteIfExists(f);
                        deleted++;
                    } catch (IOException e) {
                        logger.warn("MV compact: failed to delete input {}: {}", f, e.getMessage());
                    }
                }

                logger.info(
                    "MV compact done: target={} shard={} inputs={} deleted={} output={}",
                    targetShardId, sourceShard, snapshot.size(), deleted, finalName
                );
            } catch (Exception e) {
                // Failure: leave inputs untouched, delete tmp.
                logger.warn(
                    "MV compact failed: target={} shard={} error={}",
                    targetShardId, sourceShard, e.getMessage()
                );
                try {
                    Files.deleteIfExists(tmpPath);
                } catch (IOException ioe) {
                    logger.debug("MV compact: failed to delete tmp: {}", ioe.getMessage());
                }
            }
        } catch (IOException e) {
            logger.debug("MV compact: error listing shard dir {}: {}", shardDir, e.getMessage());
        } finally {
            compactingShards.remove(sourceShard);
        }
    }

    private void pollSourceShard(ClusterState state, IndexMetadata sourceMetadata, int sourceShard) throws IOException {
        // Find source primary node
        ShardId sourceShardId = new ShardId(sourceMetadata.getIndex(), sourceShard);
        ShardRouting primaryRouting = state.routingTable().shardRoutingTable(sourceShardId).primaryShard();
        if (primaryRouting == null || !primaryRouting.active()) {
            logger.debug("MV hydrator: source shard {} not active", sourceShardId);
            return;
        }
        DiscoveryNode primaryNode = state.nodes().get(primaryRouting.currentNodeId());
        if (primaryNode == null) {
            return;
        }

        // Send GetMVCheckpoint request to source primary (synchronous via TransportFuture)
        GetMVCheckpointRequest request = new GetMVCheckpointRequest(sourceShardId, mvId);
        GetMVCheckpointResponse response;
        try {
            response = transportService.submitRequest(
                primaryNode,
                MVCheckpointService.ACTION_GET_CHECKPOINT,
                request,
                TransportRequestOptions.builder().withTimeout(TimeValue.timeValueSeconds(10)).build(),
                new org.opensearch.transport.TransportResponseHandler<GetMVCheckpointResponse>() {
                    @Override
                    public GetMVCheckpointResponse read(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
                        return new GetMVCheckpointResponse(in);
                    }

                    @Override
                    public void handleResponse(GetMVCheckpointResponse r) {}

                    @Override
                    public void handleException(org.opensearch.transport.TransportException exp) {}

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            ).txGet();
        } catch (Exception e) {
            logger.debug("MV hydrator: GetMVCheckpoint failed for source={}: {}", sourceShardId, e.getMessage());
            return;
        }

        MVCheckpoint checkpoint = response.checkpoint();
        if (checkpoint == null) {
            return;
        }

        MVCheckpoint.MVPartialEntry entry = checkpoint.entries().get(mvId);
        if (entry == null) {
            return;
        }

        // Diff against high-water
        HighWater hw = highWaters.getOrDefault(sourceShard, HighWater.EMPTY);
        if (checkpoint.primaryTerm() < hw.term
            || (checkpoint.primaryTerm() == hw.term && entry.generation() <= hw.generation)) {
            return; // nothing new
        }

        // Download files from remote store
        Path shardHydratedDir = hydratedDir.resolve(String.valueOf(sourceShard));
        Files.createDirectories(shardHydratedDir);

        int downloadedFiles = 0;
        for (MVCheckpoint.FileInfo fileInfo : entry.files()) {
            Path targetFile = shardHydratedDir.resolve(fileInfo.name());
            if (Files.exists(targetFile) && Files.size(targetFile) == fileInfo.length()) {
                continue; // already present with correct size
            }
            try {
                downloadFromRemote(String.valueOf(sourceShard), mvId, fileInfo.name(), targetFile);
                // Verify size
                long actualSize = Files.size(targetFile);
                if (actualSize != fileInfo.length()) {
                    logger.warn(
                        "MV hydrator: size mismatch for file {} expected={} actual={}",
                        fileInfo.name(), fileInfo.length(), actualSize
                    );
                    Files.deleteIfExists(targetFile);
                    return; // abort this generation, retry next tick
                }
                downloadedFiles++;
            } catch (Exception e) {
                logger.warn("MV hydrator: download failed for file {}: {}", fileInfo.name(), e.getMessage());
                return; // retry next tick
            }
        }

        // Advance high-water
        highWaters.put(sourceShard, new HighWater(checkpoint.primaryTerm(), entry.generation()));
        logger.info(
            "MV hydrated: target={} source=[{}][{}] gen={} term={} files={} downloaded={} maxSeqNo={}",
            targetShardId, sourceIndex, sourceShard, entry.generation(),
            checkpoint.primaryTerm(), entry.files().size(), downloadedFiles, checkpoint.maxSeqNo()
        );

        // Publish hydrated files into the target engine's catalog so that
        // CatalogSnapshot.getSearchableFiles("mv_state") returns them.
        publishGeneration(sourceShard, shardHydratedDir, entry.generation(), entry.rowCount(), fileNamesOf(entry));
    }

    /** Collect the file names for a checkpoint entry. */
    private static Set<String> fileNamesOf(MVCheckpoint.MVPartialEntry entry) {
        Set<String> fileNames = new HashSet<>();
        for (MVCheckpoint.FileInfo fi : entry.files()) {
            fileNames.add(fi.name());
        }
        return fileNames;
    }

    /** Provenance key identifying this MV + source shard as a derived-artifact producer. */
    private String provenanceKey(int sourceShard) {
        return mvId + "/" + sourceShard;
    }

    /**
     * Publish a single hydrated source generation into the target catalog through the
     * allocate-generation path. Skips publication only when the target's userData marker already
     * records this (or a newer) source generation as published — the authoritative idempotency
     * signal that survives restarts. Any rejection is logged at WARN with the exception message
     * (never DEBUG), and a successful publish logs at INFO with both target and source generations.
     */
    void publishGeneration(int sourceShard, Path shardHydratedDir, long sourceGeneration, long numRows, Set<String> fileNames) {
        CatalogPublisher publisher = this.catalogPublisher;
        if (publisher == null) {
            return;
        }
        if (fileNames.isEmpty()) {
            return;
        }
        String provenanceKey = provenanceKey(sourceShard);
        try {
            long alreadyPublished = publisher.publishedSourceGeneration(provenanceKey);
            if (sourceGeneration <= alreadyPublished) {
                logger.debug(
                    "MV catalog publish skipped (marker up to date): target={} provenance={} sourceGen={} marker={}",
                    targetShardId, provenanceKey, sourceGeneration, alreadyPublished
                );
                return;
            }
            long targetGeneration = publisher.publish(
                MV_STATE_FORMAT_NAME,
                shardHydratedDir.toAbsolutePath().toString(),
                fileNames,
                provenanceKey,
                sourceGeneration,
                numRows,
                Map.of()
            );
            logger.info(
                "MV catalog published: target={} source=[{}][{}] targetGen={} sourceGen={} files={}",
                targetShardId, sourceIndex, sourceShard, targetGeneration, sourceGeneration, fileNames.size()
            );
        } catch (Exception e) {
            // A rejection here (invalid input, engine state) is real and must be visible — never
            // masked as a benign "already published" at DEBUG the way the original code did.
            logger.warn(
                "MV catalog publish rejected: target={} provenance={} sourceGen={} error={}",
                targetShardId, provenanceKey, sourceGeneration, e.getMessage()
            );
        }
    }

    private void downloadFromRemote(String shardId, String mvId, String fileName, Path targetFile) throws IOException {
        // Download data file from remote store using MVStateRemoteManager
        // (RemoteStoreReplicationSource pattern — direct remote-to-target, no source transport for files)
        Path tempFile = targetFile.resolveSibling(targetFile.getFileName() + ".tmp");
        try (InputStream is = remoteManager.downloadDataFile(shardId, mvId, fileName)) {
            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
        }
        Files.move(tempFile, targetFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Rebuild high-water from existing hydrated files on disk (restart recovery).
     */
    private void rebuildHighWater() {
        if (!Files.isDirectory(hydratedDir)) {
            return;
        }
        try (var shardDirs = Files.newDirectoryStream(hydratedDir)) {
            for (Path shardDir : shardDirs) {
                if (!Files.isDirectory(shardDir)) continue;
                int sourceShard;
                try {
                    sourceShard = Integer.parseInt(shardDir.getFileName().toString());
                } catch (NumberFormatException e) {
                    continue;
                }
                // Find highest generation from file names (partials + compacted).
                long maxGen = 0;
                try (var files = Files.newDirectoryStream(shardDir, "*.parquet")) {
                    for (Path f : files) {
                        String fname = f.getFileName().toString();
                        // Check compacted gen range first.
                        long[] range = parseCompactedGenRange(fname);
                        if (range != null) {
                            if (range[1] > maxGen) {
                                maxGen = range[1];
                            }
                        } else {
                            long gen = parseGenFromFileName(fname);
                            if (gen > maxGen) {
                                maxGen = gen;
                            }
                        }
                    }
                }
                if (maxGen > 0) {
                    highWaters.put(sourceShard, new HighWater(0, maxGen)); // term unknown, gen is key
                    logger.info("MV hydrator rebuilt high-water: source shard {} gen={}", sourceShard, maxGen);
                }
            }
        } catch (IOException e) {
            logger.warn("MV hydrator: failed to rebuild high-water from {}", hydratedDir, e);
        }
    }

    /**
     * Parse generation from partial file name: _mv_partial.s{shard}.t{term}.g{gen}.{uuid}.parquet
     */
    static long parseGenFromFileName(String name) {
        // Format: _mv_partial.s0.t1.g5.abc123.parquet
        int gIdx = name.indexOf(".g");
        if (gIdx < 0) return 0;
        int dotAfterG = name.indexOf('.', gIdx + 2);
        if (dotAfterG < 0) return 0;
        try {
            return Long.parseLong(name.substring(gIdx + 2, dotAfterG));
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * Parse compacted gen range from filename: _mv_compacted.s{shard}.g{a}-{b}.{uuid}.parquet
     * Returns [minGen, maxGen] or null if not a compacted file name.
     */
    static long[] parseCompactedGenRange(String name) {
        Matcher m = COMPACTED_NAME_PATTERN.matcher(name);
        if (!m.matches()) return null;
        try {
            long minGen = Long.parseLong(m.group(2));
            long maxGen = Long.parseLong(m.group(3));
            return new long[] { minGen, maxGen };
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Callback interface for native compaction. Implemented in the mv-engine
     * plugin layer which has access to MVNativeBridge and the definition metadata.
     */
    @FunctionalInterface
    public interface CompactCallback {
        /**
         * Compact the input files into the output file.
         * @param inputFiles absolute paths to input partial .parquet files
         * @param outputPath absolute path for output .parquet file
         */
        void compact(List<String> inputFiles, String outputPath) throws Exception;
    }

    private volatile CompactCallback compactCallback;

    /**
     * Callback interface for publishing hydrated files into the target engine's catalog.
     * Implemented by the mv-engine plugin which has access to the target IndexShard.
     *
     * <p>The engine allocates the catalog generation (the hydrator does not supply one),
     * avoiding collisions with the target's own refresh/flush/merge generations. Idempotency
     * is tracked via a provenance marker; use {@link #publishedSourceGeneration(String)} to
     * learn what has already been published and skip it.</p>
     */
    public interface CatalogPublisher {
        /**
         * Publish a set of hydrated files as a single derived-artifact generation, letting the
         * engine allocate the target catalog generation.
         *
         * @param dataFormatName  artifact format name (e.g. "mv_state")
         * @param directory       absolute path to the directory containing the files
         * @param fileNames       the file names within directory to publish
         * @param provenanceKey   upstream producer key ("&lt;mvId&gt;/&lt;sourceShard&gt;")
         * @param sourceGeneration the upstream generation being published (recorded in the marker)
         * @param numRows         total row count across all files (0 if unknown)
         * @param userDataUpdates metadata entries to merge into the catalog snapshot
         * @return the target catalog generation the engine allocated
         */
        long publish(String dataFormatName, String directory, Set<String> fileNames,
                      String provenanceKey, long sourceGeneration, long numRows,
                      Map<String, String> userDataUpdates) throws IOException;

        /**
         * Returns the maximum upstream generation already published for {@code provenanceKey}
         * on the target catalog, or {@code -1} if none.
         */
        long publishedSourceGeneration(String provenanceKey) throws IOException;
    }

    private volatile CatalogPublisher catalogPublisher;

    /**
     * Register a compaction callback. Called by the mv-engine plugin during setup.
     */
    public void setCompactCallback(CompactCallback callback) {
        this.compactCallback = callback;
    }

    /**
     * Register a catalog publisher. Called by the mv-engine plugin during setup.
     * When set, newly hydrated generations are published into the target engine's
     * catalog so that CatalogSnapshot.getSearchableFiles() returns them.
     */
    public void setCatalogPublisher(CatalogPublisher publisher) {
        this.catalogPublisher = publisher;
    }

    /**
     * Update the compact threshold (for dynamic setting changes).
     */
    public void setCompactThreshold(int threshold) {
        this.compactThreshold = threshold;
    }

    // ── Test accessors ──────────────────────────────────────────────────

    ConcurrentHashMap<Integer, HighWater> highWaters() {
        return highWaters;
    }

    boolean isClosed() {
        return closed.get();
    }

    /**
     * High-water mark for a source shard.
     */
    record HighWater(long term, long generation) {
        static final HighWater EMPTY = new HighWater(0, 0);
    }
}
