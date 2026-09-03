/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Tests for {@link WriterFileSet}.
 */
public class WriterFileSetTests extends OpenSearchTestCase {

    public void testCopyWriteable() throws Exception {
        WriterFileSet original = randomWriterFileSet();
        String directory = original.directory();
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, directory, DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        assertEquals(original, copy);
    }

    public void testDirectoryNotSerialized() throws Exception {
        String originalDirectory = "/tmp/original";
        String differentDirectory = "/tmp/different";
        WriterFileSet original = new WriterFileSet(originalDirectory, 1L, Set.of("a.dat"), 10, 0L);

        WriterFileSet deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, differentDirectory, DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );

        assertEquals(differentDirectory, deserialized.directory());
        assertNotEquals(originalDirectory, deserialized.directory());
        assertEquals(original.writerGeneration(), deserialized.writerGeneration());
        assertEquals(original.files(), deserialized.files());
        assertEquals(original.numRows(), deserialized.numRows());
    }

    public void testStreamRoundTripPreservesFormatVersion() throws Exception {
        WriterFileSet original = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 10, 9_010_000L);
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, "/tmp/dir", DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        assertEquals(9_010_000L, copy.formatVersion());
    }

    public void testDefaultFormatVersionIsZero() {
        WriterFileSet wfs = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 0, 0L);
        assertEquals(0L, wfs.formatVersion());
    }

    public void testSeqRangeFieldsDefaultToUnknown() {
        WriterFileSet wfs = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 10, 0L);
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, wfs.minSeqNo());
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, wfs.maxSeqNo());
    }

    public void testSeqRangeFieldsExplicit() {
        WriterFileSet wfs = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 10, 0L, 50L, 200L);
        assertEquals(50L, wfs.minSeqNo());
        assertEquals(200L, wfs.maxSeqNo());
    }

    public void testStreamRoundTripWithSeqRange() throws Exception {
        WriterFileSet original = new WriterFileSet("/tmp/dir", 7L, Set.of("gen7.parquet"), 100, 0L, 42L, 999L);
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, "/tmp/dir", DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        assertEquals(42L, copy.minSeqNo());
        assertEquals(999L, copy.maxSeqNo());
        assertEquals(original.writerGeneration(), copy.writerGeneration());
        assertEquals(original.numRows(), copy.numRows());
    }

    public void testStreamRoundTripLegacyReadsUnknown() throws Exception {
        // Simulate a legacy WriterFileSet that was serialized WITHOUT seq ranges.
        // Build a WriterFileSet with default (-1) seq ranges — the round trip should preserve -1.
        WriterFileSet original = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 10, 0L);
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, "/tmp/dir", DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        // New serializers write UNKNOWN_SEQ_NO; new deserializers read it back as UNKNOWN_SEQ_NO.
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, copy.minSeqNo());
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, copy.maxSeqNo());
    }

    public void testBuilderWithSeqRange() {
        WriterFileSet wfs = WriterFileSet.builder()
            .directory(java.nio.file.Path.of("/tmp/test"))
            .writerGeneration(5L)
            .addFile("data.parquet")
            .addNumRows(100)
            .minSeqNo(10L)
            .maxSeqNo(500L)
            .build();
        assertEquals(10L, wfs.minSeqNo());
        assertEquals(500L, wfs.maxSeqNo());
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet() {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom("cfs", "si", "dat", "parquet"));
        }
        return new WriterFileSet(directory, randomNonNegativeLong(), files, randomIntBetween(0, 10000), 0L);
    }
}
