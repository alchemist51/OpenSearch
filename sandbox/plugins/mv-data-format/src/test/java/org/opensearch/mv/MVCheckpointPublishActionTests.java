/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Wire round-trip tests for {@link MVCheckpointPublishAction.Request} and
 * {@link MVCheckpointPublishAction.Response}, plus {@link MVReplicationCheckpoint}
 * and {@link MVFileMetadata} serialization.
 */
public class MVCheckpointPublishActionTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws IOException {
        Map<String, MVFileMetadata> files = Map.of(
            "gen-1_0.parquet", new MVFileMetadata(1024L, 0L, 49L, 12345L),
            "gen-1_1.parquet", new MVFileMetadata(2048L, 50L, 100L, -1L)
        );
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source-idx", 0, 3L, 999L, 42L, files, System.currentTimeMillis()
        );
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "mv-target", 2, "source-uuid-123", cp
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertEquals("mv-target", deserialized.targetIndex());
        assertEquals(2, deserialized.targetShard());
        assertEquals("source-uuid-123", deserialized.sourceUuid());
        assertEquals("source-idx", deserialized.sourceIndex());
        assertEquals(0, deserialized.sourceShard());
        assertEquals(999L, deserialized.maxSeqNo());
        assertEquals(3L, deserialized.primaryTerm());
        assertEquals(42L, deserialized.infosVersion());
        assertEquals(2, deserialized.checkpoint().fileMetadata().size());
    }

    public void testRequestEmptyFiles() throws IOException {
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source", 0, 1L, 0L, 0L, Map.of(), System.currentTimeMillis()
        );
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "target", 0, "uuid", cp
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertTrue(deserialized.checkpoint().fileMetadata().isEmpty());
    }

    public void testResponseRoundTrip() throws IOException {
        MVCheckpointPublishAction.Response original = new MVCheckpointPublishAction.Response(true, 500L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Response deserialized = new MVCheckpointPublishAction.Response(in);

        assertTrue(deserialized.accepted());
        assertEquals(500L, deserialized.targetWatermark());
    }

    public void testResponseNotAccepted() throws IOException {
        MVCheckpointPublishAction.Response original = new MVCheckpointPublishAction.Response(false, -1L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Response deserialized = new MVCheckpointPublishAction.Response(in);

        assertFalse(deserialized.accepted());
        assertEquals(-1L, deserialized.targetWatermark());
    }

    public void testRequestValidation() {
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source", 0, 1L, 100L, 5L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, -1L)),
            System.currentTimeMillis()
        );
        MVCheckpointPublishAction.Request request = new MVCheckpointPublishAction.Request(
            "target", 0, "uuid", cp
        );
        assertNull(request.validate());
    }

    public void testFileMetadataRoundTrip() throws IOException {
        MVFileMetadata original = new MVFileMetadata(4096L, 100L, 200L, 0xDEADBEEFL);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVFileMetadata deserialized = new MVFileMetadata(in);

        assertEquals(4096L, deserialized.sizeBytes());
        assertEquals(100L, deserialized.minSeqNo());
        assertEquals(200L, deserialized.maxSeqNo());
        assertEquals(0xDEADBEEFL, deserialized.crc32());
        assertTrue(deserialized.hasCrc32());
    }

    public void testFileMetadataUnknownCrc() throws IOException {
        MVFileMetadata original = new MVFileMetadata(1024L, 0L, 50L, MVFileMetadata.CRC32_UNKNOWN);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVFileMetadata deserialized = new MVFileMetadata(in);

        assertFalse(deserialized.hasCrc32());
        assertEquals(-1L, deserialized.crc32());
    }

    public void testCheckpointRoundTrip() throws IOException {
        Map<String, MVFileMetadata> files = Map.of(
            "gen-1_0.parquet", new MVFileMetadata(1024L, 0L, 99L, 12345L),
            "gen-1_1.parquet", new MVFileMetadata(2048L, 100L, 199L, -1L),
            "gen-1_2.parquet", new MVFileMetadata(512L, 200L, 250L, 67890L)
        );
        MVReplicationCheckpoint original = new MVReplicationCheckpoint(
            "source-idx", 0, 1L, 250L, 10L, files, 1700000000000L
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVReplicationCheckpoint deserialized = new MVReplicationCheckpoint(in);

        assertEquals("source-idx", deserialized.sourceIndex());
        assertEquals(0, deserialized.sourceShard());
        assertEquals(1L, deserialized.primaryTerm());
        assertEquals(250L, deserialized.maxSeqNo());
        assertEquals(10L, deserialized.infosVersion());
        assertEquals(1700000000000L, deserialized.createdTimeStampMillis());
        assertEquals(3, deserialized.fileMetadata().size());

        MVFileMetadata gen0 = deserialized.fileMetadata().get("gen-1_0.parquet");
        assertNotNull(gen0);
        assertEquals(1024L, gen0.sizeBytes());
        assertEquals(0L, gen0.minSeqNo());
        assertEquals(99L, gen0.maxSeqNo());
        assertEquals(12345L, gen0.crc32());
    }

    public void testCheckpointEmptySentinel() throws IOException {
        MVReplicationCheckpoint empty = MVReplicationCheckpoint.empty("source-idx", 0);

        assertTrue(empty.isEmpty());
        assertEquals(-1L, empty.maxSeqNo());
        assertEquals(0L, empty.primaryTerm());
        assertTrue(empty.fileMetadata().isEmpty());

        // Round-trip
        BytesStreamOutput out = new BytesStreamOutput();
        empty.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVReplicationCheckpoint deserialized = new MVReplicationCheckpoint(in);
        assertTrue(deserialized.isEmpty());
    }
}
