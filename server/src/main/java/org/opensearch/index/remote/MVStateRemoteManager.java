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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Manages upload of MV partial state files to remote store, enforcing
 * the manifest-last invariant: data files first, manifest LAST.
 *
 * <p>No manifest ⟹ generation invisible. This structural guarantee
 * eliminates the defect-#30 class of visibility races.</p>
 *
 * @opensearch.internal
 */
public class MVStateRemoteManager {

    private static final Logger logger = LogManager.getLogger(MVStateRemoteManager.class);

    private final BlobContainerProvider blobContainerProvider;
    private final RemoteStorePathStrategy pathStrategy;
    private final BlobPath basePath;
    private final String indexUUID;

    /**
     * Functional interface for resolving a BlobContainer from a BlobPath.
     * Decouples from the full RemoteSegmentStoreDirectory / BlobStoreRepository.
     */
    @FunctionalInterface
    public interface BlobContainerProvider {
        BlobContainer blobContainer(BlobPath path) throws IOException;
    }

    public MVStateRemoteManager(
        BlobContainerProvider blobContainerProvider,
        RemoteStorePathStrategy pathStrategy,
        BlobPath basePath,
        String indexUUID
    ) {
        this.blobContainerProvider = blobContainerProvider;
        this.pathStrategy = pathStrategy;
        this.basePath = basePath;
        this.indexUUID = indexUUID;
    }

    /**
     * Upload sealed partial files for one MV generation, then write
     * the manifest LAST.
     *
     * @param shardId      shard ID string (e.g. "0")
     * @param mvId         materialized view identifier
     * @param primaryTerm  current primary term
     * @param generation   partial generation number
     * @param maxSeqNo     max sequence number from the refresh context
     * @param defMetadataVersion definition metadata version
     * @param definitionHash     definition hash
     * @param localFiles   local partial files to upload
     * @return the manifest that was written
     */
    public MVStateManifest uploadGeneration(
        String shardId,
        String mvId,
        long primaryTerm,
        long generation,
        long maxSeqNo,
        long defMetadataVersion,
        String definitionHash,
        Collection<Path> localFiles
    ) throws IOException {
        // 1. Upload data files
        BlobPath dataPath = resolvePath(shardId, mvId, DataType.DATA);
        BlobContainer dataContainer = blobContainerProvider.blobContainer(dataPath);

        List<MVStateManifest.FileEntry> fileEntries = new ArrayList<>();
        for (Path localFile : localFiles) {
            String fileName = localFile.getFileName().toString();
            long length = Files.size(localFile);
            // Use file size as checksum placeholder — real checksum computed at write
            String checksum = String.valueOf(length);

            try (InputStream is = Files.newInputStream(localFile)) {
                dataContainer.writeBlob(fileName, is, length, false);
            }
            fileEntries.add(new MVStateManifest.FileEntry(fileName, length, checksum));
            logger.debug("Uploaded MV data file: mvId={} gen={} file={} size={}", mvId, generation, fileName, length);
        }

        // 2. Write manifest LAST (structural invariant)
        MVStateManifest manifest = new MVStateManifest(
            primaryTerm,
            generation,
            maxSeqNo,
            defMetadataVersion,
            definitionHash,
            fileEntries
        );

        BlobPath metadataPath = resolvePath(shardId, mvId, DataType.METADATA);
        BlobContainer metadataContainer = blobContainerProvider.blobContainer(metadataPath);

        String manifestName = MVStateManifest.buildFileName(primaryTerm, generation, UUID.randomUUID().toString());
        BytesStreamOutput out = new BytesStreamOutput();
        manifest.writeTo(out);
        BytesReference bytes = out.bytes();

        try (InputStream is = bytes.streamInput()) {
            metadataContainer.writeBlob(manifestName, is, bytes.length(), false);
        }
        logger.info(
            "MV manifest uploaded: mvId={} gen={} term={} files={} manifest={}",
            mvId,
            generation,
            primaryTerm,
            fileEntries.size(),
            manifestName
        );

        return manifest;
    }

    /**
     * List manifests for an MV in the metadata path, newest-first
     * (inverted naming gives lexicographic ordering).
     *
     * @return list of manifest file names, newest generation first
     */
    public List<String> listManifests(String shardId, String mvId) throws IOException {
        BlobPath metadataPath = resolvePath(shardId, mvId, DataType.METADATA);
        BlobContainer metadataContainer = blobContainerProvider.blobContainer(metadataPath);
        List<String> manifests = new ArrayList<>(metadataContainer.listBlobs().keySet());
        Collections.sort(manifests); // Inverted names → newest first
        return manifests;
    }

    /**
     * Read and deserialize a manifest by name.
     */
    public MVStateManifest readManifest(String shardId, String mvId, String manifestName) throws IOException {
        BlobPath metadataPath = resolvePath(shardId, mvId, DataType.METADATA);
        BlobContainer metadataContainer = blobContainerProvider.blobContainer(metadataPath);
        try (InputStream is = metadataContainer.readBlob(manifestName)) {
            BytesReference bytes = org.opensearch.common.io.Streams.readFully(is);
            return new MVStateManifest(bytes.streamInput());
        }
    }

    /**
     * Resume generation counter from remote: list manifests, parse latest gen.
     *
     * @return max generation found + 1, or 1 if no manifests exist
     */
    public long resumeGeneration(String shardId, String mvId) throws IOException {
        List<String> manifests = listManifests(shardId, mvId);
        if (manifests.isEmpty()) {
            return 1;
        }
        // First entry is newest (lowest inverted number = highest real gen)
        long latestGen = MVStateManifest.parseGeneration(manifests.get(0));
        return latestGen + 1;
    }

    /**
     * Download a data file from remote store as an InputStream.
     * Used by the target hydrator (RemoteStoreReplicationSource idiom).
     *
     * @param shardId  shard ID string
     * @param mvId     materialized view identifier
     * @param fileName data file name
     * @return input stream of the file content
     */
    public InputStream downloadDataFile(String shardId, String mvId, String fileName) throws IOException {
        BlobPath dataPath = resolvePath(shardId, mvId, DataType.DATA);
        BlobContainer dataContainer = blobContainerProvider.blobContainer(dataPath);
        return dataContainer.readBlob(fileName);
    }

    private BlobPath resolvePath(String shardId, String mvId, DataType dataType) {
        MVStatePathInput.Builder builder = MVStatePathInput.builder();
        builder.mvId(mvId);
        builder.basePath(basePath);
        builder.indexUUID(indexUUID);
        builder.shardId(shardId);
        builder.dataCategory(DataCategory.MV_STATE);
        builder.dataType(dataType);
        MVStatePathInput pathInput = builder.build();
        return pathStrategy.generatePath(pathInput);
    }
}
