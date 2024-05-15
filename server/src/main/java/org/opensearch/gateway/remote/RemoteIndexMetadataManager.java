/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.gateway.remote.RemoteClusterStateService.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.RemoteStateTransferException;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;

public class RemoteIndexMetadataManager {
    public static final TimeValue INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT = TimeValue.timeValueMillis(20000);

    public static final Setting<TimeValue> INDEX_METADATA_UPLOAD_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.remote_store.state.index_metadata.upload_timeout",
        INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        IndexMetadata::fromXContent
    );
    public static final String INDEX_PATH_TOKEN = "index";

    private final BlobStoreRepository blobStoreRepository;

    private volatile TimeValue indexMetadataUploadTimeout;

    public RemoteIndexMetadataManager(BlobStoreRepository blobStoreRepository, ClusterSettings clusterSettings) {
        this.blobStoreRepository = blobStoreRepository;
        this.indexMetadataUploadTimeout = clusterSettings.get(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING);
        clusterSettings.addSettingsUpdateConsumer(INDEX_METADATA_UPLOAD_TIMEOUT_SETTING, this::setIndexMetadataUploadTimeout);
    }

    /**
     * Allows async Upload of IndexMetadata to remote
     *
     * @param clusterState current ClusterState
     * @param indexMetadata {@link IndexMetadata} to upload
     * @param latchedActionListener listener to respond back on after upload finishes
     */
    CheckedRunnable<IOException> getIndexMetadataAsyncAction(
        ClusterState clusterState,
        IndexMetadata indexMetadata,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        final BlobContainer indexMetadataContainer = indexMetadataContainer(
            clusterState.getClusterName().value(),
            clusterState.metadata().clusterUUID(),
            indexMetadata.getIndexUUID()
        );
        final String indexMetadataFilename = indexMetadataFileName(indexMetadata);
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedIndexMetadata(
                    indexMetadata.getIndex().getName(),
                    indexMetadata.getIndexUUID(),
                    indexMetadataContainer.path().buildAsString() + indexMetadataFilename
                )
            ),
            ex -> latchedActionListener.onFailure(new RemoteStateTransferException(indexMetadata.getIndex().toString(), ex))
        );

        return () -> INDEX_METADATA_FORMAT.writeAsyncWithUrgentPriority(
            indexMetadata,
            indexMetadataContainer,
            indexMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );
    }

    /**
     * Fetch index metadata from remote cluster state
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param uploadedIndexMetadata {@link ClusterMetadataManifest.UploadedIndexMetadata} contains details about remote location of index metadata
     * @return {@link IndexMetadata}
     */
    IndexMetadata getIndexMetadata(
        String clusterName,
        String clusterUUID,
        ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata
    ) {
        BlobContainer blobContainer = indexMetadataContainer(clusterName, clusterUUID, uploadedIndexMetadata.getIndexUUID());
        try {
            String[] splitPath = uploadedIndexMetadata.getUploadedFilename().split("/");
            return INDEX_METADATA_FORMAT.read(
                blobContainer,
                splitPath[splitPath.length - 1],
                blobStoreRepository.getNamedXContentRegistry()
            );
        } catch (IOException e) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "Error while downloading IndexMetadata - %s", uploadedIndexMetadata.getUploadedFilename()),
                e
            );
        }
    }

    /**
     * Fetch latest index metadata from remote cluster state
     *
     * @param clusterUUID uuid of cluster state to refer to in remote
     * @param clusterName name of the cluster
     * @param clusterMetadataManifest manifest file of cluster
     * @return {@code Map<String, IndexMetadata>} latest IndexUUID to IndexMetadata map
     */
    Map<String, IndexMetadata> getIndexMetadataMap(
        String clusterName,
        String clusterUUID,
        ClusterMetadataManifest clusterMetadataManifest
    ) {
        assert Objects.equals(clusterUUID, clusterMetadataManifest.getClusterUUID())
            : "Corrupt ClusterMetadataManifest found. Cluster UUID mismatch.";
        Map<String, IndexMetadata> remoteIndexMetadata = new HashMap<>();
        for (ClusterMetadataManifest.UploadedIndexMetadata uploadedIndexMetadata : clusterMetadataManifest.getIndices()) {
            IndexMetadata indexMetadata = getIndexMetadata(clusterName, clusterUUID, uploadedIndexMetadata);
            remoteIndexMetadata.put(uploadedIndexMetadata.getIndexUUID(), indexMetadata);
        }
        return remoteIndexMetadata;
    }

    private BlobContainer indexMetadataContainer(String clusterName, String clusterUUID, String indexUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/ftqsCnn9TgOX
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID).add(INDEX_PATH_TOKEN).add(indexUUID));
    }

    public TimeValue getIndexMetadataUploadTimeout() {
        return this.indexMetadataUploadTimeout;
    }

    private void setIndexMetadataUploadTimeout(TimeValue newIndexMetadataUploadTimeout) {
        this.indexMetadataUploadTimeout = newIndexMetadataUploadTimeout;
    }

    static String indexMetadataFileName(IndexMetadata indexMetadata) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/index/<index_UUID>/metadata__<inverted_index_metadata_version>__<inverted__timestamp>__<codec
        // version>
        return String.join(
            DELIMITER,
            METADATA_FILE_PREFIX,
            RemoteStoreUtils.invertLong(indexMetadata.getVersion()),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION) // Keep the codec version at last place only, during read we reads last
            // place to determine codec version.
        );
    }
}
