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
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.mv.MVCheckpointRequestAction;
import org.opensearch.mv.MVFileMetadata;
import org.opensearch.mv.MVReplicationCheckpoint;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

/**
 * MV-specific implementation of {@link DerivedSourceReader}. Owns the
 * {@link MVRemoteSource} for name-addressed file downloads from the
 * source index's remote segment store.
 *
 * <h2>Push-first, cold-start RPC</h2>
 * <p>When the source publishes a checkpoint advert via {@link MVCheckpointMailbox},
 * {@code fetchSnapshot} consumes the mailbox and uses name-addressed
 * {@link MVRemoteSource#downloadFiles} to fetch ONLY the referenced files
 * (no remote listing). If the mailbox is empty:</p>
 * <ul>
 *   <li><b>Seeded (had a previous successful round):</b> return null immediately
 *       (no remote IO). The next push will populate the mailbox.</li>
 *   <li><b>Not seeded (cold start):</b> send a cold-start request via
 *       {@link MVCheckpointRequestAction} to the source node. The source
 *       handler reads its local catalog and replies with a full checkpoint.
 *       On RPC failure, return null — the poller's existing backoff will
 *       retry on the next round.</li>
 * </ul>
 *
 * <p>There is no legacy pull path. All data arrives via push (mailbox) or
 * cold-start RPC.</p>
 */
final class MVDerivedSourceReader implements DerivedSourceReader {

    private static final Logger logger = LogManager.getLogger(MVDerivedSourceReader.class);

    /** Cold-start RPC timeout. The poller thread is GENERIC; a short block is acceptable. */
    private static final long COLD_START_TIMEOUT_SECONDS = 5;

    private final IndexSettings indexSettings;
    private final MVPullSettings.Services services;
    private volatile MVRemoteSource source;
    private volatile org.apache.lucene.store.NIOFSDirectory workingDirectory;
    private volatile Path workingPath;
    /** True after the first successful round seeds the reader. */
    private volatile boolean seeded = false;

    MVDerivedSourceReader(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.services = services;
    }

    @Override
    public DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) throws IOException {
        ensureInitialized(shard);

        // ── Push-first: check the mailbox for a source-pushed checkpoint ─────
        MVCheckpointMailbox mailbox = MVCheckpointMailbox.instance();
        if (mailbox == null) {
            // Mailbox not initialized — cannot proceed
            logger.warn("mv_pull: checkpoint mailbox not initialized");
            return null;
        }

        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        if (binding == null) {
            return null;
        }

        String sourceIndexName = binding.sourceName();
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        MVReplicationCheckpoint pushed = mailbox.consume(
            indexSettings.getIndex().getName(),
            shard.shardId().id(),
            sourceIndexName,
            sourceShardId
        );

        if (pushed != null) {
            if (pushed.maxSeqNo() <= sinceWatermark) {
                logger.debug(
                    "MAILBOX_STALE target=[{}][{}] pushed_maxSeqNo={} <= watermark={}",
                    indexSettings.getIndex().getName(),
                    shard.shardId().id(),
                    pushed.maxSeqNo(),
                    sinceWatermark
                );
                return null;
            }
            // Use the pushed checkpoint — name-addressed download with CRC verification
            return fetchFromCheckpoint(shard, pushed);
        }

        // ── Mailbox empty ────────────────────────────────────────────
        if (seeded) {
            logger.trace(
                "MAILBOX_EMPTY_SEEDED target=[{}][{}] watermark={} — no-op",
                indexSettings.getIndex().getName(),
                shard.shardId().id(),
                sinceWatermark
            );
            return null;
        }

        // ── Cold start: RPC to source node ───────────────────────────
        logger.debug(
            "COLD_START target=[{}][{}] watermark={}",
            indexSettings.getIndex().getName(),
            shard.shardId().id(),
            sinceWatermark
        );

        MVReplicationCheckpoint coldStart = tryColdStartRequest(
            sourceIndexName, sourceShardId, shard
        );
        if (coldStart != null) {
            if (coldStart.maxSeqNo() <= sinceWatermark) {
                seeded = true;
                return null;
            }
            DerivedSourceSnapshot snapshot = fetchFromCheckpoint(shard, coldStart);
            if (snapshot != null) {
                seeded = true;
            }
            return snapshot;
        }

        // Cold-start RPC failed — return null, poller's backoff retries next round.
        logger.debug(
            "COLD_START_FAILED target=[{}][{}] — will retry next round",
            indexSettings.getIndex().getName(),
            shard.shardId().id()
        );
        return null;
    }

    /**
     * Creates a snapshot from a checkpoint using name-addressed download with
     * CRC32 verification. After fetching each file, if the checkpoint carries
     * a known CRC32, we compute the local CRC32 and verify; mismatch triggers
     * delete + single retry, then fails the round.
     */
    private DerivedSourceSnapshot fetchFromCheckpoint(ShardRouting shard, MVReplicationCheckpoint checkpoint) throws IOException {
        Map<String, MVFileMetadata> fileMeta = checkpoint.fileMetadata();
        if (fileMeta.isEmpty()) {
            logger.warn(
                "PUSH_CHECKPOINT_NO_FILES target=[{}][{}] maxSeqNo={}",
                indexSettings.getIndex().getName(),
                shard.shardId().id(),
                checkpoint.maxSeqNo()
            );
            return null;
        }

        List<String> neededFiles = new ArrayList<>(fileMeta.keySet());
        List<Path> parquetFiles = source.downloadFiles(neededFiles, workingPath);
        if (parquetFiles.isEmpty()) {
            logger.warn(
                "PUSH_CHECKPOINT_DOWNLOAD_FAILED target=[{}][{}] maxSeqNo={} requested={}",
                indexSettings.getIndex().getName(),
                shard.shardId().id(),
                checkpoint.maxSeqNo(),
                neededFiles.size()
            );
            return null;
        }

        // CRC32 verification for files that carry a known checksum
        List<Path> verifiedFiles = new ArrayList<>(parquetFiles.size());
        for (Path downloaded : parquetFiles) {
            String fileName = downloaded.getFileName().toString();
            // Reverse the download name mangling: '$' was substituted for '/'
            String originalName = fileName.replace('$', '/');
            MVFileMetadata meta = fileMeta.get(originalName);
            if (meta == null) {
                // Try the direct name (no mangling in some paths)
                meta = fileMeta.get(fileName);
            }
            if (meta != null && meta.hasCrc32()) {
                long expectedCrc32 = meta.crc32();
                long actualCrc32 = computeFileCrc32(downloaded);
                if (actualCrc32 != expectedCrc32) {
                    logger.warn(
                        "CHECKSUM_MISMATCH file=[{}] expected={} actual={} — deleting and retrying",
                        fileName, expectedCrc32, actualCrc32
                    );
                    MVBuildMetrics.INSTANCE.recordCrcVerifyFailed();
                    Files.deleteIfExists(downloaded);
                    // Single retry
                    List<Path> retried = source.downloadFiles(List.of(originalName), workingPath);
                    if (retried.isEmpty()) {
                        logger.error("CHECKSUM_MISMATCH_RETRY_FAILED file=[{}]", fileName);
                        continue; // Skip this file — fail the round if critical
                    }
                    Path retriedPath = retried.get(0);
                    long retryCrc32 = computeFileCrc32(retriedPath);
                    if (retryCrc32 != expectedCrc32) {
                        logger.error(
                            "CHECKSUM_MISMATCH_PERSISTENT file=[{}] expected={} retried={}",
                            fileName, expectedCrc32, retryCrc32
                        );
                        MVBuildMetrics.INSTANCE.recordCrcVerifyFailed();
                        Files.deleteIfExists(retriedPath);
                        // Fail the round — return null to let the poller retry
                        return null;
                    }
                    MVBuildMetrics.INSTANCE.recordCrcVerifyPassed();
                    verifiedFiles.add(retriedPath);
                } else {
                    MVBuildMetrics.INSTANCE.recordCrcVerifyPassed();
                    verifiedFiles.add(downloaded);
                }
            } else {
                // No CRC available — pass through without verification
                verifiedFiles.add(downloaded);
            }
        }

        if (verifiedFiles.isEmpty()) {
            return null;
        }

        seeded = true;
        return new MVSourceSnapshot(
            shard.shardId().toString(),
            checkpoint.maxSeqNo(),
            checkpoint.primaryTerm(),
            checkpoint.infosVersion(),
            verifiedFiles
        );
    }

    /**
     * Computes CRC32 of a local file. Cheap sequential read of a just-downloaded file.
     */
    private static long computeFileCrc32(Path file) throws IOException {
        CRC32 crc = new CRC32();
        byte[] buffer = new byte[8192];
        try (InputStream is = Files.newInputStream(file)) {
            int n;
            while ((n = is.read(buffer)) >= 0) {
                crc.update(buffer, 0, n);
            }
        }
        return crc.getValue();
    }

    /**
     * Cold-start RPC: sends {@link MVCheckpointRequestAction} to the source
     * primary node. The handler runs on GENERIC (verified in
     * {@link org.opensearch.mv.MVCheckpointRequestTransportHandler} — registered
     * with {@code ThreadPool.Names.GENERIC} executor) so there is no deadlock
     * even when source and target are on the same node: the request executes on
     * a different GENERIC thread than this caller.
     *
     * <p>Blocks for up to {@link #COLD_START_TIMEOUT_SECONDS}. On any failure,
     * returns null — the poller's existing backoff retries next round.</p>
     */
    private MVReplicationCheckpoint tryColdStartRequest(
        String sourceIndexName,
        int sourceShardId,
        ShardRouting shard
    ) {
        org.opensearch.transport.client.Client client = services.client();
        if (client == null) {
            logger.debug("COLD_START_REQUEST: client not available (test context?)");
            return null;
        }

        try {
            // Resolve the source primary node from cluster state
            org.opensearch.cluster.routing.ShardRouting sourceRouting = services.clusterService()
                .state()
                .routingTable()
                .shardRoutingTable(sourceIndexName, sourceShardId)
                .primaryShard();
            if (sourceRouting == null || !sourceRouting.active()) {
                logger.debug("COLD_START_REQUEST: source primary not available for [{}][{}]", sourceIndexName, sourceShardId);
                return null;
            }

            MVCheckpointRequestAction.Request request = new MVCheckpointRequestAction.Request(
                sourceIndexName,
                sourceShardId,
                indexSettings.getIndex().getName(),
                shard.shardId().id()
            );

            logger.debug(
                "COLD_START_REQUEST target=[{}][{}] -> source=[{}][{}]",
                indexSettings.getIndex().getName(),
                shard.shardId().id(),
                sourceIndexName,
                sourceShardId
            );

            // Synchronous with bounded timeout. The handler is registered on
            // GENERIC executor (not SAME), so even co-located source/target
            // won't deadlock — the handler runs on a separate GENERIC thread.
            MVCheckpointRequestAction.Response response = client.execute(
                MVCheckpointRequestAction.INSTANCE,
                request
            ).actionGet(COLD_START_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (response.available() && response.checkpoint() != null) {
                logger.info(
                    "COLD_START_REPLY target=[{}][{}] maxSeqNo={} files={}",
                    indexSettings.getIndex().getName(),
                    shard.shardId().id(),
                    response.checkpoint().maxSeqNo(),
                    response.checkpoint().fileMetadata().size()
                );
                return response.checkpoint();
            } else {
                logger.debug(
                    "COLD_START_REPLY: source has no data for [{}][{}]",
                    sourceIndexName,
                    sourceShardId
                );
                return null;
            }
        } catch (Exception e) {
            logger.debug(
                "COLD_START_REQUEST failed for [{}][{}]: {}",
                sourceIndexName,
                sourceShardId,
                e.getMessage()
            );
            return null;
        }
    }

    @Override
    public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException {
        MVSourceSnapshot mvSnapshot = (MVSourceSnapshot) snapshot;
        for (Path parquetFile : mvSnapshot.parquetFiles()) {
            Path target = stageDir.resolve(parquetFile.getFileName().toString());
            Files.copy(parquetFile, target);
        }
    }

    @Override
    public void close() throws IOException {
        if (workingDirectory != null) {
            workingDirectory.close();
        }
    }

    private void ensureInitialized(ShardRouting shard) throws IOException {
        if (source != null) {
            return;
        }
        Settings settings = indexSettings.getSettings();
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(settings);
        if (binding == null) {
            throw new IllegalStateException("mv_pull: target shard [" + shard.shardId() + "] has no DerivedIndexBinding");
        }
        services.resolveAndValidateSource(binding);
        String sourceIndexName = binding.sourceName();
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        binding.validateTargetTopology(indexSettings.getNumberOfShards());

        this.source = new MVRemoteSource(services, sourceIndexName, sourceShardId, binding);

        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));
        this.workingPath = Files.createTempDirectory(tmpDir, "mv_pull_reader");
        this.workingDirectory = new org.apache.lucene.store.NIOFSDirectory(workingPath);
    }

    /**
     * Opaque snapshot carrying MV-specific advert metadata alongside the
     * parquet file list.
     */
    static final class MVSourceSnapshot implements DerivedSourceSnapshot {
        private final String shardId;
        private final long maxSeqNo;
        private final long primaryTerm;
        private final long infosVersion;
        private final List<Path> parquetFiles;

        MVSourceSnapshot(String shardId, long maxSeqNo, long primaryTerm, long infosVersion, List<Path> parquetFiles) {
            this.shardId = shardId;
            this.maxSeqNo = maxSeqNo;
            this.primaryTerm = primaryTerm;
            this.infosVersion = infosVersion;
            this.parquetFiles = List.copyOf(parquetFiles);
        }

        @Override
        public String shardId() {
            return shardId;
        }

        @Override
        public long watermark() {
            return maxSeqNo;
        }

        @Override
        public Map<String, String> metadata() {
            return Map.of("primaryTerm", Long.toString(primaryTerm), "infosVersion", Long.toString(infosVersion));
        }

        long primaryTerm() {
            return primaryTerm;
        }

        long infosVersion() {
            return infosVersion;
        }

        List<Path> parquetFiles() {
            return parquetFiles;
        }
    }
}
