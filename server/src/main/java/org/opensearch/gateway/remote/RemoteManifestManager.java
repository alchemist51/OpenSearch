/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;

public class RemoteManifestManager {
    public static final String MANIFEST_PATH_TOKEN = "manifest";
    public static final String MANIFEST_FILE_PREFIX = "manifest";
    public static final String METADATA_MANIFEST_NAME_FORMAT = "%s";
    public static final int MANIFEST_CURRENT_CODEC_VERSION = ClusterMetadataManifest.CODEC_V2;
    public static final int SPLITED_MANIFEST_FILE_LENGTH = 6; // file name manifest__term__version__C/P__timestamp__codecversion

    public static final TimeValue METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.metadata_manifest.upload_timeout",
        METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Manifest format compatible with older codec v0, where codec version was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V0 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV0);
    /**
     * Manifest format compatible with older codec v1, where global metadata was missing.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT_V1 =
        new ChecksumBlobStoreFormat<>("cluster-metadata-manifest", METADATA_MANIFEST_NAME_FORMAT, ClusterMetadataManifest::fromXContentV1);

    /**
     * Manifest format compatible with codec v2, where we introduced codec versions/global metadata.
     */
    public static final ChecksumBlobStoreFormat<ClusterMetadataManifest> CLUSTER_METADATA_MANIFEST_FORMAT = new ChecksumBlobStoreFormat<>(
        "cluster-metadata-manifest",
        METADATA_MANIFEST_NAME_FORMAT,
        ClusterMetadataManifest::fromXContent
    );

    private final BlobStoreRepository blobStoreRepository;
    private volatile TimeValue metadataManifestUploadTimeout;
    private final String nodeId;
    private static final Logger logger = LogManager.getLogger(RemoteManifestManager.class);

    RemoteManifestManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings, String nodeId) {
        this.blobStoreRepository = blobStoreRepository;
        this.metadataManifestUploadTimeout = clusterSettings.get(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING);
        this.nodeId = nodeId;
        clusterSettings.addSettingsUpdateConsumer(METADATA_MANIFEST_UPLOAD_TIMEOUT_SETTING, this::setMetadataManifestUploadTimeout);
    }

    private ClusterMetadataManifest uploadV1Manifest(
        ClusterState clusterState,
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadata,
        String previousClusterUUID,
        String globalMetadataFileName,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String manifestFileName = getManifestFileName(
                clusterState.term(),
                clusterState.version(),
                committed,
                ClusterMetadataManifest.CODEC_V1
            );
            ClusterMetadataManifest manifest = ClusterMetadataManifest.builder()
                .clusterTerm(clusterState.term())
                .stateVersion(clusterState.getVersion())
                .clusterUUID(clusterState.metadata().clusterUUID())
                .stateUUID(clusterState.stateUUID())
                .opensearchVersion(Version.CURRENT)
                .nodeId(nodeId)
                .committed(committed)
                .codecVersion(ClusterMetadataManifest.CODEC_V1)
                .globalMetadataFileName(globalMetadataFileName)
                .indices(uploadedIndexMetadata)
                .previousClusterUUID(previousClusterUUID)
                .clusterUUIDCommitted(clusterState.metadata().clusterUUIDCommitted())
                .build();
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest, manifestFileName);
            return manifest;
        }
    }

    ClusterMetadataManifest uploadManifest(
        ClusterState clusterState,
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadata,
        String previousClusterUUID,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedCoordinationMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedSettingsMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedTemplatesMetadata,
        Map<String, ClusterMetadataManifest.UploadedMetadataAttribute> uploadedCustomMetadataMap,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedDiscoveryNodesMetadata,
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedClusterBlocksMetadata,
        ClusterMetadataManifest.ClusterDiffManifest clusterDiffManifest,
        boolean committed
    ) throws IOException {
        synchronized (this) {
            final String manifestFileName = getManifestFileName(
                clusterState.term(),
                clusterState.version(),
                committed,
                MANIFEST_CURRENT_CODEC_VERSION
            );
            final ClusterMetadataManifest manifest = new ClusterMetadataManifest(
                clusterState.term(),
                clusterState.getVersion(),
                clusterState.metadata().clusterUUID(),
                clusterState.stateUUID(),
                Version.CURRENT,
                nodeId,
                committed,
                MANIFEST_CURRENT_CODEC_VERSION,
                null,
                uploadedIndexMetadata,
                previousClusterUUID,
                clusterState.metadata().clusterUUIDCommitted(),
                uploadedCoordinationMetadata,
                uploadedSettingsMetadata,
                uploadedTemplatesMetadata,
                uploadedCustomMetadataMap,
                uploadedDiscoveryNodesMetadata,
                uploadedClusterBlocksMetadata,
                clusterDiffManifest
            );
            writeMetadataManifest(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID(), manifest, manifestFileName);
            return manifest;
        }
    }

    private void writeMetadataManifest(String clusterName, String clusterUUID, ClusterMetadataManifest uploadManifest, String fileName)
        throws IOException {
        AtomicReference<String> result = new AtomicReference<String>();
        AtomicReference<Exception> exceptionReference = new AtomicReference<Exception>();

        final BlobContainer metadataManifestContainer = manifestContainer(clusterName, clusterUUID);

        // latch to wait until upload is not finished
        CountDownLatch latch = new CountDownLatch(1);

        LatchedActionListener completionListener = new LatchedActionListener<>(ActionListener.wrap(resp -> {
            logger.trace(String.format(Locale.ROOT, "Manifest file uploaded successfully."));
        }, ex -> { exceptionReference.set(ex); }), latch);

        getClusterMetadataManifestBlobStoreFormat(fileName).writeAsyncWithUrgentPriority(
            uploadManifest,
            metadataManifestContainer,
            fileName,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );

        try {
            if (latch.await(getMetadataManifestUploadTimeout().millis(), TimeUnit.MILLISECONDS) == false) {
                RemoteStateTransferException ex = new RemoteStateTransferException(
                    String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete")
                );
                throw ex;
            }
        } catch (InterruptedException ex) {
            RemoteStateTransferException exception = new RemoteStateTransferException(
                String.format(Locale.ROOT, "Timed out waiting for transfer of manifest file to complete - %s"),
                ex
            );
            Thread.currentThread().interrupt();
            throw exception;
        }
        if (exceptionReference.get() != null) {
            throw new RemoteStateTransferException(exceptionReference.get().getMessage(), exceptionReference.get());
        }
        logger.debug(
            "Metadata manifest file [{}] written during [{}] phase. ",
            fileName,
            uploadManifest.isCommitted() ? "commit" : "publish"
        );
    }

    /**
     * Fetch latest ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    public Optional<ClusterMetadataManifest> getLatestClusterMetadataManifest(String clusterName, String clusterUUID) {
        Optional<String> latestManifestFileName = getLatestManifestFileName(clusterName, clusterUUID);
        return latestManifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    public Optional<ClusterMetadataManifest> getClusterMetadataManifestByTermVersion(String clusterName, String clusterUUID, long term, long version) {
        Optional<String> manifestFileName = getManifestFileNameByTermVersion(clusterName, clusterUUID, term, version);
        return manifestFileName.map(s -> fetchRemoteClusterMetadataManifest(clusterName, clusterUUID, s));
    }

    /**
     * Fetch ClusterMetadataManifest from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return ClusterMetadataManifest
     */
    ClusterMetadataManifest fetchRemoteClusterMetadataManifest(String clusterName, String clusterUUID, String filename)
        throws IllegalStateException {
        try {
            return getClusterMetadataManifestBlobStoreFormat(filename).read(
                manifestContainer(clusterName, clusterUUID),
                filename,
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(String.format(Locale.ROOT, "Error while downloading cluster metadata - %s", filename), e);
        }
    }

    Map<String, ClusterMetadataManifest> getLatestManifestForAllClusterUUIDs(String clusterName, Set<String> clusterUUIDs) {
        Map<String, ClusterMetadataManifest> manifestsByClusterUUID = new HashMap<>();
        for (String clusterUUID : clusterUUIDs) {
            try {
                Optional<ClusterMetadataManifest> manifest = getLatestClusterMetadataManifest(clusterName, clusterUUID);
                manifest.ifPresent(clusterMetadataManifest -> manifestsByClusterUUID.put(clusterUUID, clusterMetadataManifest));
            } catch (Exception e) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "Exception in fetching manifest for clusterUUID: %s", clusterUUID),
                    e
                );
            }
        }
        return manifestsByClusterUUID;
    }

    private ChecksumBlobStoreFormat<ClusterMetadataManifest> getClusterMetadataManifestBlobStoreFormat(String fileName) {
        long codecVersion = getManifestCodecVersion(fileName);
        if (codecVersion == MANIFEST_CURRENT_CODEC_VERSION) {
            return CLUSTER_METADATA_MANIFEST_FORMAT;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V1) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V1;
        } else if (codecVersion == ClusterMetadataManifest.CODEC_V0) {
            return CLUSTER_METADATA_MANIFEST_FORMAT_V0;
        }

        throw new IllegalArgumentException("Cluster metadata manifest file is corrupted, don't have valid codec version");
    }

    private int getManifestCodecVersion(String fileName) {
        String[] splitName = fileName.split(DELIMITER);
        if (splitName.length == SPLITED_MANIFEST_FILE_LENGTH) {
            return Integer.parseInt(splitName[splitName.length - 1]); // Last value would be codec version.
        } else if (splitName.length < SPLITED_MANIFEST_FILE_LENGTH) { // Where codec is not part of file name, i.e. default codec version 0
            // is used.
            return ClusterMetadataManifest.CODEC_V0;
        } else {
            throw new IllegalArgumentException("Manifest file name is corrupted");
        }
    }

    private BlobContainer manifestContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest
        return blobStoreRepository.blobStore().blobContainer(getManifestFolderPath(clusterName, clusterUUID));
    }

    BlobPath getManifestFolderPath(String clusterName, String clusterUUID) {
        return getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID).add(MANIFEST_PATH_TOKEN);
    }

    public TimeValue getMetadataManifestUploadTimeout() {
        return this.metadataManifestUploadTimeout;
    }

    private void setMetadataManifestUploadTimeout(TimeValue newMetadataManifestUploadTimeout) {
        this.metadataManifestUploadTimeout = newMetadataManifestUploadTimeout;
    }

    /**
     * Fetch ClusterMetadataManifest files from remote state store in order
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param limit max no of files to fetch
     * @return all manifest file names
     */
    private List<BlobMetadata> getManifestFileNames(String clusterName, String clusterUUID, String filePrefix, int limit) throws IllegalStateException {
        try {

            /*
              {@link BlobContainer#listBlobsByPrefixInSortedOrder} will list the latest manifest file first
              as the manifest file name generated via {@link RemoteClusterStateService#getManifestFileName} ensures
              when sorted in LEXICOGRAPHIC order the latest uploaded manifest file comes on top.
             */
            return manifestContainer(clusterName, clusterUUID).listBlobsByPrefixInSortedOrder(
                filePrefix,
                limit,
                BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC
            );
        } catch (IOException e) {
            throw new IllegalStateException("Error while fetching latest manifest file for remote cluster state", e);
        }
    }

    static String getManifestFileName(long term, long version, boolean committed, int codecVersion) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/manifest/manifest__<inverted_term>__<inverted_version>__C/P__<inverted__timestamp>__<codec_version>
        return String.join(
            DELIMITER,
            MANIFEST_PATH_TOKEN,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version),
            (committed ? "C" : "P"), // C for committed and P for published
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(codecVersion) // Keep the codec version at last place only, during read we reads last place to
            // determine codec version.
        );
    }

    static String getManifestFilePrefixForTermVersion(long term, long version) {
        return String.join(
            DELIMITER,
            MANIFEST_FILE_PREFIX,
            RemoteStoreUtils.invertLong(term),
            RemoteStoreUtils.invertLong(version)
        ) + DELIMITER;
    }

    /**
     * Fetch latest ClusterMetadataManifest file from remote state store
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @return latest ClusterMetadataManifest filename
     */
    private Optional<String> getLatestManifestFileName(String clusterName, String clusterUUID) throws IllegalStateException {
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, MANIFEST_FILE_PREFIX + DELIMITER, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}", clusterName, clusterUUID);
        return Optional.empty();
    }

    private Optional<String> getManifestFileNameByTermVersion(String clusterName, String clusterUUID, long term, long version) throws IllegalStateException {
        final String filePrefix = getManifestFilePrefixForTermVersion(term, version);
        List<BlobMetadata> manifestFilesMetadata = getManifestFileNames(clusterName, clusterUUID, filePrefix, 1);
        if (manifestFilesMetadata != null && !manifestFilesMetadata.isEmpty()) {
            return Optional.of(manifestFilesMetadata.get(0).name());
        }
        logger.info("No manifest file present in remote store for cluster name: {}, cluster UUID: {}, term: {}, version: {}", clusterName, clusterUUID, term, version);
        return Optional.empty();
    }
}
