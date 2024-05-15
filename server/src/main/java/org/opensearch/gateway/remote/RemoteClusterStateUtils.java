/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteClusterStateUtils {
    public static final String METADATA_NAME_FORMAT = "%s.dat";
    public static final String METADATA_FILE_PREFIX = "metadata";
    public static final String CLUSTER_STATE_PATH_TOKEN = "cluster-state";
    public static final String DELIMITER = "__";

    // ToXContent Params with gateway mode.
    // We are using gateway context mode to persist all custom metadata.
    public static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(1);
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new ToXContent.MapParams(params);
    }

    public static BlobPath getCusterMetadataBasePath(BlobStoreRepository blobStoreRepository, String clusterName, String clusterUUID) {
        return blobStoreRepository.basePath().add(encodeString(clusterName)).add(CLUSTER_STATE_PATH_TOKEN).add(clusterUUID);
    }

    public static String encodeString(String content) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(content.getBytes(StandardCharsets.UTF_8));
    }

    static BlobContainer clusterUUIDContainer(BlobStoreRepository blobStoreRepository, String clusterName) {
        return blobStoreRepository.blobStore()
            .blobContainer(
                blobStoreRepository.basePath()
                    .add(Base64.getUrlEncoder().withoutPadding().encodeToString(clusterName.getBytes(StandardCharsets.UTF_8)))
                    .add(CLUSTER_STATE_PATH_TOKEN)
            );
    }

    /**
     * Exception for Remote state transfer.
     */
    static class RemoteStateTransferException extends RuntimeException {

        public RemoteStateTransferException(String errorDesc) {
            super(errorDesc);
        }

        public RemoteStateTransferException(String errorDesc, Throwable cause) {
            super(errorDesc, cause);
        }
    }

    static class UploadedMetadataResults {
        List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadata;
        Map<String, ClusterMetadataManifest.UploadedMetadataAttribute> uploadedCustomMetadataMap;
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedCoordinationMetadata;
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedSettingsMetadata;
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedTemplatesMetadata;
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedDiscoveryNodes;
        ClusterMetadataManifest.UploadedMetadataAttribute uploadedClusterBlocks;

        public UploadedMetadataResults(
            List<ClusterMetadataManifest.UploadedIndexMetadata> uploadedIndexMetadata,
            Map<String, ClusterMetadataManifest.UploadedMetadataAttribute> uploadedCustomMetadataMap,
            ClusterMetadataManifest.UploadedMetadataAttribute uploadedCoordinationMetadata,
            ClusterMetadataManifest.UploadedMetadataAttribute uploadedSettingsMetadata,
            ClusterMetadataManifest.UploadedMetadataAttribute uploadedTemplatesMetadata,
            ClusterMetadataManifest.UploadedMetadataAttribute uploadedDiscoveryNodes,
            ClusterMetadataManifest.UploadedMetadataAttribute uploadedClusterBlocks
        ) {
            this.uploadedIndexMetadata = uploadedIndexMetadata;
            this.uploadedCustomMetadataMap = uploadedCustomMetadataMap;
            this.uploadedCoordinationMetadata = uploadedCoordinationMetadata;
            this.uploadedSettingsMetadata = uploadedSettingsMetadata;
            this.uploadedTemplatesMetadata = uploadedTemplatesMetadata;
            this.uploadedDiscoveryNodes = uploadedDiscoveryNodes;
            this.uploadedClusterBlocks = uploadedClusterBlocks;
        }

        public UploadedMetadataResults() {
            this.uploadedIndexMetadata = new ArrayList<>();
            this.uploadedCustomMetadataMap = new HashMap<>();
            this.uploadedCoordinationMetadata = null;
            this.uploadedSettingsMetadata = null;
            this.uploadedTemplatesMetadata = null;
            this.uploadedDiscoveryNodes = null;
            this.uploadedClusterBlocks = null;
        }
    }
}
