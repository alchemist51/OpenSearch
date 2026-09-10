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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Remote-store source: reads the SOURCE index's remote segment store
 * for name-addressed file downloads. The source shard is never contacted
 * directly — all data arrives via the remote segment store.
 *
 * <h2>Name-addressed download</h2>
 * <p>{@link #downloadFiles(List, Path)} fetches exactly the named parquet files
 * from the remote directory without requiring a full metadata listing. The
 * remote directory handle is initialized once and cached across rounds. On a
 * name miss (file not in the directory's internal cache), a SINGLE re-init
 * retry is attempted; if that fails, the file is skipped.</p>
 */
final class MVRemoteSource {

    private static final Logger logger = LogManager.getLogger(MVRemoteSource.class);

    private final MVPullSettings.Services services;
    private final String sourceIndexName;
    private final int sourceShardId;
    private final org.opensearch.cluster.metadata.DerivedIndexBinding binding;
    private RemoteSegmentStoreDirectory remoteDirectory;

    /**
     * Cached metadata from the last successful {@code remote.init()} call.
     */
    private volatile RemoteSegmentMetadata cachedMetadata;

    private long metadataCacheRefreshes;
    private long nameAddressedDownloads;
    private long nameAddressedReinits;

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

    /**
     * Name-addressed file download: fetches exactly the named parquet files
     * from the remote segment store WITHOUT a full metadata listing.
     *
     * <p>Uses the CACHED remote directory handle (initialized once). On a
     * file name miss (NoSuchFileException), does a single re-init retry
     * to refresh the remote metadata cache, then retries the file. If the
     * retry fails, the file is skipped with a warning.</p>
     *
     * @param fileNames  the remote parquet file names to download
     * @param destDir    the local directory to download into
     * @return the list of successfully downloaded local file paths
     */
    List<Path> downloadFiles(List<String> fileNames, Path destDir) throws IOException {
        if (fileNames.isEmpty()) {
            return List.of();
        }
        RemoteSegmentStoreDirectory remote = remoteDirectory();

        // Ensure the remote directory has been initialized at least once
        if (cachedMetadata == null) {
            cachedMetadata = remote.init();
            metadataCacheRefreshes++;
        }

        List<Path> downloaded = new ArrayList<>();
        boolean reinitAttempted = false;

        for (String fileName : fileNames) {
            nameAddressedDownloads++;
            Path localFile = destDir.resolve(fileName.replace('/', '$'));
            if (Files.exists(localFile)) {
                downloaded.add(localFile);
                continue;
            }

            try {
                if (downloadParallel(remote, fileName, localFile) == false) {
                    // Try to copy from remote using the cached metadata (single stream)
                    copySingleStream(remote, fileName, destDir, localFile);
                }
                downloaded.add(localFile);
            } catch (java.io.FileNotFoundException | java.nio.file.NoSuchFileException e) {
                // Name miss — the file is not in the remote directory's cache.
                // Try ONE re-init to refresh the metadata.
                if (!reinitAttempted) {
                    reinitAttempted = true;
                    nameAddressedReinits++;
                    try {
                        cachedMetadata = remote.init();
                        metadataCacheRefreshes++;
                        // Retry the download through the same parallel-first path: the file that misses right after a
                        // source merge is typically the multi-GB merged output, and a single stream costs 65-82 s for it.
                        if (downloadParallel(remote, fileName, localFile) == false) {
                            copySingleStream(remote, fileName, destDir, localFile);
                        }
                        downloaded.add(localFile);
                    } catch (Exception retryEx) {
                        logger.warn("mv_pull NAME_ADDRESSED_MISS file=[{}] after re-init: {}", fileName, retryEx.getMessage());
                    }
                } else {
                    logger.warn("mv_pull NAME_ADDRESSED_MISS file=[{}] (re-init already attempted)", fileName);
                }
            }
        }
        return downloaded;
    }

    long getMetadataCacheRefreshes() {
        return metadataCacheRefreshes;
    }

    /** Files at least this large are downloaded part-wise in parallel (S3 multipart objects expose their parts). */
    private static final long PARALLEL_DOWNLOAD_MIN_BYTES = Long.getLong(
        "opensearch.mv_pull.download.parallel_min_bytes",
        32L * 1024 * 1024
    );
    private static final int PARALLEL_DOWNLOAD_STREAMS = Integer.getInteger("opensearch.mv_pull.download.streams", 8);
    private static final long PARALLEL_DOWNLOAD_TIMEOUT_SECONDS = Long.getLong("opensearch.mv_pull.download.timeout_seconds", 900L);

    private void copySingleStream(RemoteSegmentStoreDirectory remote, String fileName, Path destDir, Path localFile) throws IOException {
        long start = System.nanoTime();
        try (var dir = new org.apache.lucene.store.NIOFSDirectory(destDir)) {
            dir.copyFrom(remote, fileName, fileName.replace('/', '$'), org.apache.lucene.store.IOContext.DEFAULT);
        }
        long bytes = Files.exists(localFile) ? Files.size(localFile) : -1L;
        long ms = Math.max(1L, (System.nanoTime() - start) / 1_000_000L);
        if (bytes >= PARALLEL_DOWNLOAD_MIN_BYTES) {
            logger.info(
                "mv_pull DOWNLOAD file=[{}] bytes={} ms={} MB/s={} streams=1",
                fileName,
                bytes,
                ms,
                String.format(java.util.Locale.ROOT, "%.0f", bytes / 1e6 / (ms / 1000.0))
            );
        }
    }

    /**
     * Downloads {@code fileName} into {@code localFile} with several concurrent part streams when the file is large and the
     * repository's blob container supports async multi-part reads (S3 does for multipart-uploaded objects). Returns false when
     * the parallel path does not apply or failed before writing anything, so the caller falls back to the single stream.
     */
    private boolean downloadParallel(RemoteSegmentStoreDirectory remote, String fileName, Path localFile) {
        if (PARALLEL_DOWNLOAD_STREAMS <= 1) {
            return false;
        }
        RemoteSegmentStoreDirectory.UploadedSegmentMetadata meta = remote.getSegmentsUploadedToRemoteStore().get(fileName);
        if (meta == null || meta.getLength() < PARALLEL_DOWNLOAD_MIN_BYTES) {
            return false;
        }
        long start = System.nanoTime();
        try {
            org.opensearch.action.support.PlainActionFuture<String> done = org.opensearch.action.support.PlainActionFuture.newFuture();
            remote.copyToParallel(fileName, localFile, services.threadPool(), PARALLEL_DOWNLOAD_STREAMS, done);
            done.actionGet(PARALLEL_DOWNLOAD_TIMEOUT_SECONDS, java.util.concurrent.TimeUnit.SECONDS);
            long ms = Math.max(1L, (System.nanoTime() - start) / 1_000_000L);
            long bytes = Files.size(localFile);
            logger.info(
                "mv_pull DOWNLOAD file=[{}] bytes={} ms={} MB/s={} streams={}",
                fileName,
                bytes,
                ms,
                String.format(java.util.Locale.ROOT, "%.0f", bytes / 1e6 / (ms / 1000.0)),
                PARALLEL_DOWNLOAD_STREAMS
            );
            return true;
        } catch (Exception e) {
            if (e instanceof UnsupportedOperationException || e.getCause() instanceof UnsupportedOperationException) {
                return false; // repository without multi-part reads: plain single stream, no warning
            }
            StringBuilder chain = new StringBuilder();
            for (Throwable t = e; t != null && chain.length() < 600; t = t.getCause()) {
                chain.append(chain.length() == 0 ? "" : " <- ").append(t.getClass().getSimpleName()).append(": ").append(t.getMessage());
            }
            logger.warn(
                "mv_pull parallel download of [{}] failed after {} ms, falling back to a single stream: {}",
                fileName,
                (System.nanoTime() - start) / 1_000_000L,
                chain
            );
            try {
                Files.deleteIfExists(localFile);
            } catch (IOException ignored) {}
            return false;
        }
    }

    long getNameAddressedDownloads() {
        return nameAddressedDownloads;
    }

    long getNameAddressedReinits() {
        return nameAddressedReinits;
    }

    private RemoteSegmentStoreDirectory remoteDirectory() throws IOException {
        if (remoteDirectory == null) {
            IndexMetadata sourceMetadata = services.sourceIndexMetadata(sourceIndexName);
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
            RemoteSegmentStoreDirectoryFactory factory = new RemoteSegmentStoreDirectoryFactory(
                services.repositoriesService(),
                services.threadPool(),
                services.segmentsPathFixedPrefix()
            );
            org.opensearch.index.IndexSettings sourceIndexSettings = new org.opensearch.index.IndexSettings(
                sourceMetadata,
                services.clusterService().getSettings()
            );
            org.apache.lucene.store.Directory directory = factory.newDirectory(
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
