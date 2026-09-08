/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

/**
 * Defect #30 (cross-node advert/visibility race) unit tests for
 * {@link MVCheckpointRequestTransportHandler#filesetRemoteVisible}: the source
 * must advertise a fileset only when every file of it is visible in the remote
 * store's uploaded map. The partial-fileset case is the exact race observed on
 * the two-node S3 cluster (source advertised generations whose S3 upload had
 * not completed; the target staged a partial range and failed coverage with
 * small constant shortfalls).
 */
public class MVCheckpointVisibilityTests extends OpenSearchTestCase {

    private static final String FORMAT = "parquet";

    private static WriterFileSet fileset(String... files) {
        return new WriterFileSet("/tmp/x", 1L, Set.of(files), 100L, 1L);
    }

    private static String key(String file) {
        return FileMetadata.serialize(FORMAT, file);
    }

    public void testAllFilesVisibleIsAdvertisable() {
        WriterFileSet wfs = fileset("_parquet_file_generation_a.parquet", "_parquet_file_generation_b.parquet");
        Set<String> visible = Set.of(key("_parquet_file_generation_a.parquet"), key("_parquet_file_generation_b.parquet"));
        assertTrue(MVCheckpointRequestTransportHandler.filesetRemoteVisible(wfs, FORMAT, visible));
    }

    /** THE race: one file of the generation not yet uploaded — whole fileset must be withheld. */
    public void testPartiallyUploadedFilesetIsWithheld() {
        WriterFileSet wfs = fileset("_parquet_file_generation_a.parquet", "_parquet_file_generation_b.parquet");
        Set<String> visible = Set.of(key("_parquet_file_generation_a.parquet"));
        assertFalse(MVCheckpointRequestTransportHandler.filesetRemoteVisible(wfs, FORMAT, visible));
    }

    public void testNothingUploadedYetWithholdsEverything() {
        WriterFileSet wfs = fileset("_parquet_file_generation_a.parquet");
        assertFalse(MVCheckpointRequestTransportHandler.filesetRemoteVisible(wfs, FORMAT, Set.of()));
    }

    /** Non-remote source (local-only dev): fail open — pre-#30 behaviour preserved. */
    public void testNullVisibilitySetFailsOpen() {
        WriterFileSet wfs = fileset("_parquet_file_generation_a.parquet");
        assertTrue(MVCheckpointRequestTransportHandler.filesetRemoteVisible(wfs, FORMAT, null));
    }

    /** Keys must be compared in FileMetadata.serialize form — raw names must not match. */
    public void testRawFileNamesDoNotMatchSerializedKeys() {
        WriterFileSet wfs = fileset("_parquet_file_generation_a.parquet");
        Set<String> rawNamesOnly = Set.of("_parquet_file_generation_a.parquet");
        assertFalse(MVCheckpointRequestTransportHandler.filesetRemoteVisible(wfs, FORMAT, rawNamesOnly));
    }
}
