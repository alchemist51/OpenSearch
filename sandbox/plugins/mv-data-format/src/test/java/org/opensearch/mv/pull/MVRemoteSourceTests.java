/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

public class MVRemoteSourceTests extends OpenSearchTestCase {

    public void testRequiredPullFilesIncludeParquetAndSegmentInfo() {
        assertTrue(MVRemoteSource.isRequiredPullFile("parquet/_parquet_file_generation_1.parquet"));
        assertTrue(MVRemoteSource.isRequiredPullFile("lucene/_0.si"));
    }

    public void testRequiredPullFilesExcludeUnusedLuceneBlobs() {
        assertFalse(MVRemoteSource.isRequiredPullFile("lucene/_0.tim"));
        assertFalse(MVRemoteSource.isRequiredPullFile("lucene/_0.tip"));
        assertFalse(MVRemoteSource.isRequiredPullFile("lucene/_0.dvd"));
        assertFalse(MVRemoteSource.isRequiredPullFile("lucene/_0.fdm"));
        assertFalse(MVRemoteSource.isRequiredPullFile("segments_42"));
    }
}
