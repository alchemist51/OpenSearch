/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;

public class RemoteIndexMetadataManagerTests extends OpenSearchTestCase {
    private RemoteIndexMetadataManager remoteIndexMetadataManager;
    private BlobStoreRepository blobStoreRepository;
    private ClusterSettings clusterSettings;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        blobStoreRepository = mock(BlobStoreRepository.class);
        remoteIndexMetadataManager = new RemoteIndexMetadataManager(blobStoreRepository, clusterSettings);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testIndexMetadataUploadWaitTimeSetting() {
        // verify default value
        assertEquals(
            RemoteIndexMetadataManager.INDEX_METADATA_UPLOAD_TIMEOUT_DEFAULT,
            remoteIndexMetadataManager.getIndexMetadataUploadTimeout()
        );

        // verify update index metadata upload timeout
        int indexMetadataUploadTimeout = randomIntBetween(1, 10);
        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.state.index_metadata.upload_timeout", indexMetadataUploadTimeout + "s")
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(indexMetadataUploadTimeout, remoteIndexMetadataManager.getIndexMetadataUploadTimeout().seconds());
    }
}
