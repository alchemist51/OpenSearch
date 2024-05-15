/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.opensearch.gateway.remote.RemoteClusterStateService.INDEX_METADATA_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteClusterStateServiceTests.generateClusterStateWithOneIndex;
import static org.opensearch.gateway.remote.RemoteClusterStateServiceTests.nodesWithLocalNodeClusterManager;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.DELIMITER;
import static org.opensearch.gateway.remote.RemoteClusterStateUtils.METADATA_FILE_PREFIX;
import static org.opensearch.gateway.remote.RemoteManifestManager.MANIFEST_CURRENT_CODEC_VERSION;
import static org.opensearch.gateway.remote.RemoteManifestManager.MANIFEST_FILE_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteManifestManagerTests extends OpenSearchTestCase {
    private RemoteManifestManager remoteManifestManager;
    private ClusterSettings clusterSettings;
    private BlobStoreRepository blobStoreRepository;
    private BlobStore blobStore;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        remoteManifestManager = new RemoteManifestManager(blobStoreRepository, clusterSettings, "test-node-id");
        blobStore = mock(BlobStore.class);
        when(blobStoreRepository.blobStore()).thenReturn(blobStore);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testMetadataManifestUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteManifestManager.METADATA_MANIFEST_UPLOAD_TIMEOUT_DEFAULT,
            remoteManifestManager.getMetadataManifestUploadTimeout()
        );

        // verify update metadata manifest upload timeout
        int metadataManifestUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.metadata_manifest.upload_timeout", metadataManifestUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(metadataManifestUploadTimeout, remoteManifestManager.getMetadataManifestUploadTimeout().seconds());
    }

    public void testFileNames() {
        final Index index = new Index("test-index", "index-uuid");
        final Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();
        final IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        String indexMetadataFileName = RemoteIndexMetadataManager.indexMetadataFileName(indexMetadata);
        String[] splittedIndexMetadataFileName = indexMetadataFileName.split(DELIMITER);
        assertEquals(4, indexMetadataFileName.split(DELIMITER).length);
        assertEquals(METADATA_FILE_PREFIX, splittedIndexMetadataFileName[0]);
        assertEquals(RemoteStoreUtils.invertLong(indexMetadata.getVersion()), splittedIndexMetadataFileName[1]);
        assertEquals(String.valueOf(INDEX_METADATA_CURRENT_CODEC_VERSION), splittedIndexMetadataFileName[3]);

        verifyManifestFileNameWithCodec(MANIFEST_CURRENT_CODEC_VERSION);
        verifyManifestFileNameWithCodec(ClusterMetadataManifest.CODEC_V1);
        verifyManifestFileNameWithCodec(ClusterMetadataManifest.CODEC_V0);
    }

    private void verifyManifestFileNameWithCodec(int codecVersion) {
        int term = randomIntBetween(5, 10);
        int version = randomIntBetween(5, 10);
        String manifestFileName = RemoteManifestManager.getManifestFileName(term, version, true, codecVersion);
        assertEquals(6, manifestFileName.split(DELIMITER).length);
        String[] splittedName = manifestFileName.split(DELIMITER);
        assertEquals(MANIFEST_FILE_PREFIX, splittedName[0]);
        assertEquals(RemoteStoreUtils.invertLong(term), splittedName[1]);
        assertEquals(RemoteStoreUtils.invertLong(version), splittedName[2]);
        assertEquals("C", splittedName[3]);
        assertEquals(String.valueOf(codecVersion), splittedName[5]);

        manifestFileName = RemoteManifestManager.getManifestFileName(term, version, false, codecVersion);
        splittedName = manifestFileName.split(DELIMITER);
        assertEquals("P", splittedName[3]);
    }

    public void testReadLatestMetadataManifestFailedIOException() throws IOException {
        final ClusterState clusterState = generateClusterStateWithOneIndex().nodes(nodesWithLocalNodeClusterManager()).build();

        BlobContainer blobContainer = mockBlobStoreObjects();
        when(blobContainer.listBlobsByPrefixInSortedOrder("manifest" + DELIMITER, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC))
            .thenThrow(IOException.class);

        Exception e = assertThrows(
            IllegalStateException.class,
            () -> remoteManifestManager.getLatestClusterMetadataManifest(
                clusterState.getClusterName().value(),
                clusterState.metadata().clusterUUID()
            )
        );
        assertEquals(e.getMessage(), "Error while fetching latest manifest file for remote cluster state");
    }

    private BlobContainer mockBlobStoreObjects() {
        final BlobPath blobPath = mock(BlobPath.class);
        when((blobStoreRepository.basePath())).thenReturn(blobPath);
        when(blobPath.add(anyString())).thenReturn(blobPath);
        when(blobPath.buildAsString()).thenReturn("/blob/path/");
        final BlobContainer blobContainer = mock(BlobContainer.class);
        when(blobContainer.path()).thenReturn(blobPath);
        when(blobStore.blobContainer(any())).thenReturn(blobContainer);
        return blobContainer;
    }
}
