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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

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
        } catch (Exception e) {
            logger.warn("MV hydrator poll cycle failed: target={} error={}", targetShardId, e.getMessage());
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
                // Find highest generation from file names
                long maxGen = 0;
                try (var files = Files.newDirectoryStream(shardDir, "*.parquet")) {
                    for (Path f : files) {
                        long gen = parseGenFromFileName(f.getFileName().toString());
                        if (gen > maxGen) {
                            maxGen = gen;
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
