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
                // Try to copy from remote using the cached metadata
                try (var dir = new org.apache.lucene.store.NIOFSDirectory(destDir)) {
                    dir.copyFrom(remote, fileName, fileName.replace('/', '$'), org.apache.lucene.store.IOContext.DEFAULT);
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
                        // Retry the download
                        try (var dir = new org.apache.lucene.store.NIOFSDirectory(destDir)) {
                            dir.copyFrom(remote, fileName, fileName.replace('/', '$'), org.apache.lucene.store.IOContext.DEFAULT);
                        }
                        downloaded.add(localFile);
                    } catch (Exception retryEx) {
                        logger.warn(
                            "mv_pull NAME_ADDRESSED_MISS file=[{}] after re-init: {}",
                            fileName,
                            retryEx.getMessage()
                        );
                    }
                } else {
                    logger.warn(
                        "mv_pull NAME_ADDRESSED_MISS file=[{}] (re-init already attempted)",
                        fileName
                    );
                }
            }
        }
        return downloaded;
    }

    long getMetadataCacheRefreshes() {
        return metadataCacheRefreshes;
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
