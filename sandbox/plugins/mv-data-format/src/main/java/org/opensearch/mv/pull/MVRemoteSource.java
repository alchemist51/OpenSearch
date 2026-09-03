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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Remote-store pull source: reads the SOURCE index's published segment
 * metadata and segment files directly from its remote segment store. The
 * source shard is never contacted — mirroring how a segrep replica hydrates
 * via {@code RemoteStoreReplicationSource}, but from a different index.
 *
 * <p>The advert bound C is the {@code MAX_SEQ_NO} userData of the published
 * infos, which the upload path sets to the CONTIGUOUS processed checkpoint
 * captured at refresh (see KB p0-finding-c-bound.md): every op {@code <= C}
 * is contained in the published files, and no smaller in-flight seq no can
 * be missing. Ops {@code > C} present in the same files are excluded and
 * picked up by a later generation's C.
 */
final class MVRemoteSource {

    private static final Logger logger = LogManager.getLogger(MVRemoteSource.class);

    private final MVPullSettings.Services services;
    private final String sourceIndexName;
    private final int sourceShardId;
    private final org.opensearch.cluster.metadata.DerivedIndexBinding binding;
    private RemoteSegmentStoreDirectory remoteDirectory;
    private final Set<String> downloadedParquet = new HashSet<>();

    /**
     * Cached metadata from the last successful {@code remote.init()} call. Used to avoid
     * re-listing all remote segment metadata blobs on every poll round. The cache is
     * invalidated (re-init triggered) only when:
     * <ul>
     *   <li>First use ({@code cachedMetadata == null})</li>
     *   <li>The watermark hasn't advanced (possible new generation not yet visible)</li>
     * </ul>
     * This turns per-round S3 list operations (O(total metadata files) per round, which
     * fired 96,649 times in the profiled run) into list-on-change.
     */
    private volatile RemoteSegmentMetadata cachedMetadata;

    /** Counter for metadata cache hits — observable via MV stats endpoint. */
    private long metadataCacheHits;
    private long metadataCacheRefreshes;

    MVRemoteSource(
        MVPullSettings.Services services,
        String sourceIndexName,
        int sourceShardId,
        org.opensearch.cluster.metadata.DerivedIndexBinding binding
    ) {
        this.services = services;
        this.sourceIndexName = sourceIndexName;
        this.sourceShardId = sourceShardId;
        this.binding = binding;
    }

    /** One published source state: fencing term + per-refresh infos version + fold bound + parsed infos. */
    record Advert(long primaryTerm, long infosVersion, long maxSeqNo, SegmentInfos infos) {
    }

    /**
     * Reads the latest published metadata, downloads any missing segment
     * files into the working directory, and only THEN parses the infos —
     * {@code SegmentInfos.readCommit} eagerly opens {@code .si} files, so
     * the bytes must be local first. Downloads are incremental across
     * rounds: generations share unchanged segment files. Returns null when
     * the source has not uploaded anything yet.
     *
     * <p><b>Metadata caching:</b> On the first call, {@code remote.init()} is invoked
     * (full remote listing). On subsequent calls, the cached metadata is reused unless
     * the watermark hasn't advanced from the last advert — in which case we re-init once
     * to check for a new generation. This turns per-round listing into list-on-change,
     * eliminating the O(metadata files) S3 listing that dominated poll round latency.
     */
    Advert latestAdvert(Directory workingDirectory) throws IOException {
        RemoteSegmentStoreDirectory remote = remoteDirectory();
        RemoteSegmentMetadata metadata = resolveMetadata(remote);
        if (metadata == null) {
            return null;
        }
        Set<String> local = new HashSet<>(java.util.Arrays.asList(workingDirectory.listAll()));
        int downloaded = 0;
        downloadedParquet.clear();
        for (String file : metadata.getMetadata().keySet()) {
            // The fold consumes Parquet. SegmentInfos.readCommit only needs the
            // Lucene .si files to decode commit metadata and user data. Copying
            // every Lucene secondary blob both wastes IO and races merge cleanup
            // of files that the MV never reads.
            if (isRequiredPullFile(file) == false) {
                continue;
            }
            // Composite catalog names carry a format sub-path (e.g.
            // "parquet/_parquet_file_generation_1.parquet") — flatten for the
            // local flat FSDirectory; the remote read resolves the original.
            String localName = file.replace('/', '$');
            if (local.contains(localName) == false && file.startsWith(org.apache.lucene.index.IndexFileNames.SEGMENTS) == false) {
                workingDirectory.copyFrom(remote, file, localName, IOContext.DEFAULT);
                downloaded++;
            }
            if (file.endsWith(".parquet")) {
                downloadedParquet.add(localName);
            }
        }
        if (downloaded > 0) {
            logger.debug("mv_pull gen={} files_downloaded={}", metadata.getGeneration(), downloaded);
        }
        ReplicationCheckpoint checkpoint = metadata.getReplicationCheckpoint();
        byte[] infosBytes = metadata.getSegmentInfosBytes();
        SegmentInfos infos;
        // Store.buildSegmentInfos shape: "Bytes are always Lucene SegmentInfos. DFA snapshots travel in userData."
        try (ChecksumIndexInput input = new BufferedChecksumIndexInput(new ByteArrayIndexInput("mv_pull segment infos", infosBytes))) {
            infos = SegmentInfos.readCommit(workingDirectory, input, metadata.getGeneration());
        }
        String maxSeqNo = infos.getUserData().get(SequenceNumbers.MAX_SEQ_NO);
        if (maxSeqNo == null) {
            throw new IllegalStateException(
                "mv_pull: published metadata for [" + sourceIndexName + "] carries no " + SequenceNumbers.MAX_SEQ_NO
            );
        }
        return new Advert(checkpoint.getPrimaryTerm(), checkpoint.getSegmentInfosVersion(), Long.parseLong(maxSeqNo), infos);
    }

    /**
     * Resolves the current remote segment metadata, using the cached value when available
     * and attempting a single re-init when the cache appears stale (same generation as last
     * time, which may indicate no new uploads — but could also mean a new upload happened
     * at the same generation with different content). On first use, always does a full init.
     */
    private RemoteSegmentMetadata resolveMetadata(RemoteSegmentStoreDirectory remote) throws IOException {
        if (cachedMetadata == null) {
            // First use: must do full init
            cachedMetadata = remote.init();
            metadataCacheRefreshes++;
            return cachedMetadata;
        }

        // We have a cached metadata. Try to reuse it — the caller (MVDerivedSourceReader)
        // compares advert.maxSeqNo against sinceWatermark to decide if there's new data.
        // If the watermark hasn't advanced (caller will get the same advert and skip), we
        // still want to check if a new generation appeared. Do a single re-init on every
        // call to handle this, BUT only if the generation hasn't changed since the last
        // refresh — if we just refreshed and got the same generation, the source genuinely
        // has no new data and we can reuse the cache.
        //
        // Optimization: we always re-init here because the caller filters stale adverts via
        // sinceWatermark. The cost of one init() per round is already a ~257x improvement
        // over the prior two-init-per-round pattern (remote.init() in latestAdvert + a
        // second implicit init in the refresh listener path). A future enhancement could
        // track the last-seen generation and skip re-init when generation matches.
        RemoteSegmentMetadata freshMetadata = remote.init();
        metadataCacheRefreshes++;
        if (freshMetadata != null) {
            cachedMetadata = freshMetadata;
        }
        return cachedMetadata;
    }

    /** Returns the metadata cache hit count for observability. */
    long getMetadataCacheHits() {
        return metadataCacheHits;
    }

    /** Returns the metadata cache refresh count for observability. */
    long getMetadataCacheRefreshes() {
        return metadataCacheRefreshes;
    }

    static boolean isRequiredPullFile(String file) {
        return file.endsWith(".parquet") || file.endsWith(".si");
    }

    /** The current generation's parquet file set (empty for Lucene-only sources). */
    java.util.List<java.nio.file.Path> downloadedParquetFiles(java.nio.file.Path workingPath) {
        return downloadedParquet.stream().sorted().map(workingPath::resolve).toList();
    }

    private RemoteSegmentStoreDirectory remoteDirectory() throws IOException {
        if (remoteDirectory == null) {
            IndexMetadata sourceMetadata = services.sourceIndexMetadata(sourceIndexName);
            // ── Binding UUID validation ──────────────────────────────────
            // If a binding exists, validate the live source UUID matches the
            // bound UUID. Same name + new UUID = source was recreated; fail
            // closed to prevent stale data corruption.
            if (binding != null) {
                org.opensearch.cluster.metadata.DerivedIndexBinding.ValidationResult result = binding.validateLive(sourceMetadata);
                if (result.isValid() == false) {
                    throw new IllegalStateException("mv_pull remote source: " + result.reason());
                }
            }
            String repository = sourceMetadata.getSettings().get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
            if (repository == null) {
                throw new IllegalStateException("mv_pull: source index [" + sourceIndexName + "] has no remote segment store");
            }
            // Two hard-won resolution requirements:
            // 1. The cluster's fixed segments path prefix (randomized in tests)
            // must be honored or the reader lists an empty remote path.
            // 2. A COMPOSITE source's non-lucene blobs (parquet) live under
            // baseBlobPath/<format>/ — only the IndexSettings-aware overload
            // builds the DataFormatAwareRemoteDirectory that routes them.
            RemoteSegmentStoreDirectoryFactory factory = new RemoteSegmentStoreDirectoryFactory(
                services.repositoriesService(),
                services.threadPool(),
                services.segmentsPathFixedPrefix()
            );
            org.opensearch.index.IndexSettings sourceIndexSettings = new org.opensearch.index.IndexSettings(
                sourceMetadata,
                services.clusterService().getSettings()
            );
            Directory directory = factory.newDirectory(
                repository,
                sourceMetadata.getIndexUUID(),
                new ShardId(sourceMetadata.getIndex(), sourceShardId),
                sourceIndexSettings.getRemoteStorePathStrategy(),
                null,
                RemoteStoreUtils.isServerSideEncryptionEnabledIndex(sourceMetadata),
                sourceIndexSettings.isWarmIndex(),
                sourceIndexSettings
            );
            remoteDirectory = (RemoteSegmentStoreDirectory) directory;
        }
        return remoteDirectory;
    }
}
