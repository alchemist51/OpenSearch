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
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;

import java.io.IOException;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.FORMAT_PARAMS;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_NAME_FORMAT;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.getCusterMetadataBasePath;

public class RemoteClusterStateAttributesManager {
    public static final String DISCOVERY_NODES = "nodes";
    public static final String CLUSTER_BLOCKS = "blocks";
    public static final String CUSTOM_PREFIX = "custom";
    public static final ChecksumBlobStoreFormat<DiscoveryNodes> DISCOVERY_NODES_FORMAT = new ChecksumBlobStoreFormat<>(
        "nodes",
        METADATA_NAME_FORMAT,
        DiscoveryNodes::fromXContent
    );
    public static final ChecksumBlobStoreFormat<ClusterBlocks> CLUSTER_BLOCKS_FORMAT = new ChecksumBlobStoreFormat<>(
        "blocks",
        METADATA_NAME_FORMAT,
        ClusterBlocks::fromXContent
    );
    public static final int CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION = 1;
    private final BlobStoreRepository blobStoreRepository;

    RemoteClusterStateAttributesManager(BlobStoreRepository repository) {
        blobStoreRepository = repository;
    }

    /**
     * Allows async upload of Cluster State Attribute components to remote
     */
    CheckedRunnable<IOException> getAsyncMetadataWriteAction(
        ClusterState clusterState,
        String component,
        ChecksumBlobStoreFormat componentMetadataBlobStore,
        ToXContent componentData,
        LatchedActionListener<ClusterMetadataManifest.UploadedMetadata> latchedActionListener
    ) {
        final BlobContainer remoteStateAttributeContainer = clusterStateAttributeContainer(clusterState.getClusterName().value(), clusterState.metadata().clusterUUID());
        final String componentMetadataFilename = clusterStateAttributeFileName(component, clusterState.metadata().version());
        ActionListener<Void> completionListener = ActionListener.wrap(
            resp -> latchedActionListener.onResponse(
                new ClusterMetadataManifest.UploadedMetadataAttribute(component, componentMetadataFilename)
            ),
            ex -> latchedActionListener.onFailure(new RemoteClusterStateUtils.RemoteStateTransferException(component, ex))
        );
        return () -> componentMetadataBlobStore.writeAsyncWithUrgentPriority(
            componentData,
            remoteStateAttributeContainer,
            componentMetadataFilename,
            blobStoreRepository.getCompressor(),
            completionListener,
            FORMAT_PARAMS
        );
    }

    private BlobContainer clusterStateAttributeContainer(String clusterName, String clusterUUID) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/
        return blobStoreRepository.blobStore()
            .blobContainer(getCusterMetadataBasePath(blobStoreRepository, clusterName, clusterUUID));
    }

    private static String clusterStateAttributeFileName(String componentPrefix, Long metadataVersion) {
        // 123456789012_test-cluster/cluster-state/dsgYj10Nkso7/<componentPrefix>__<inverted_metadata_version>__<inverted__timestamp>__<codec_version>
        return String.join(
            DELIMITER,
            componentPrefix,
            RemoteStoreUtils.invertLong(metadataVersion),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(CLUSTER_STATE_ATTRIBUTES_CURRENT_CODEC_VERSION)
        );
    }
}
