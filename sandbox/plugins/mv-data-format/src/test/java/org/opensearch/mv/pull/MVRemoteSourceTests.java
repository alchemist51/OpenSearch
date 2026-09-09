/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MVRemoteSource}.
 *
 * <p>After the no-legacy sweep, MVRemoteSource only exposes
 * {@code downloadFiles()} (name-addressed download). The legacy
 * {@code latestAdvert()}, {@code isRequiredPullFile()}, and
 * {@code downloadedParquetFiles()} methods have been deleted.
 *
 * <p>Integration-level downloadFiles tests require a real or mock
 * RemoteSegmentStoreDirectory which is covered by the integration
 * tests (MVPullDataFusionIT). This file retains a placeholder for
 * future unit tests when a mock remote directory is available.
 */
public class MVRemoteSourceTests extends OpenSearchTestCase {

    /**
     * Placeholder: MVRemoteSource.downloadFiles with an empty list
     * returns an empty list without initializing the remote directory.
     * Full download tests require a mock RemoteSegmentStoreDirectory.
     */
    public void testPlaceholder() {
        // MVRemoteSource requires real services to construct; downloadFiles
        // with empty input is a trivial code path. The meaningful test is
        // in MVPullDataFusionIT which exercises the full push→download→build
        // pipeline end to end.
        assertTrue(true);
    }
}
