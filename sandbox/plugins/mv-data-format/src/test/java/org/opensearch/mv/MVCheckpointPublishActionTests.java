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
import java.util.List;

/**
 * Wire round-trip tests for {@link MVCheckpointPublishAction.Request} and
 * {@link MVCheckpointPublishAction.Response}.
 */
public class MVCheckpointPublishActionTests extends OpenSearchTestCase {

    public void testRequestRoundTrip() throws IOException {
        List<String> files = List.of("gen-1_0.parquet", "gen-1_1.parquet");
        List<Long> sizes = List.of(1024L, 2048L);
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "mv-target",
            2,
            "source-idx",
            "source-uuid-123",
            0,
            999L,
            3L,
            42L,
            files,
            sizes
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertEquals("mv-target", deserialized.targetIndex());
        assertEquals(2, deserialized.targetShard());
        assertEquals("source-idx", deserialized.sourceIndex());
        assertEquals("source-uuid-123", deserialized.sourceUuid());
        assertEquals(0, deserialized.sourceShard());
        assertEquals(999L, deserialized.maxSeqNo());
        assertEquals(3L, deserialized.primaryTerm());
        assertEquals(42L, deserialized.infosVersion());
        assertEquals(files, deserialized.parquetFiles());
        assertEquals(sizes, deserialized.fileSizes());
    }

    public void testRequestEmptyFiles() throws IOException {
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "target",
            0,
            "source",
            "uuid",
            0,
            0L,
            1L,
            0L,
            List.of(),
            List.of()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertTrue(deserialized.parquetFiles().isEmpty());
        assertTrue(deserialized.fileSizes().isEmpty());
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
        MVCheckpointPublishAction.Request request = new MVCheckpointPublishAction.Request(
            "target",
            0,
            "source",
            "uuid",
            0,
            100L,
            1L,
            5L,
            List.of("a.parquet"),
            List.of(1024L)
        );
        assertNull(request.validate());
    }

    public void testRequestWithNegativeFileSizes() throws IOException {
        // -1 means size unknown — valid wire format
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "target",
            0,
            "source",
            "uuid",
            0,
            100L,
            1L,
            5L,
            List.of("a.parquet", "b.parquet"),
            List.of(-1L, -1L)
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertEquals(List.of(-1L, -1L), deserialized.fileSizes());
    }

    public void testRequestRoundTripWithSeqRanges() throws IOException {
        List<String> files = List.of("gen-1_0.parquet", "gen-1_1.parquet", "gen-1_2.parquet");
        List<Long> sizes = List.of(1024L, 2048L, 512L);
        List<Long> minSeqs = List.of(0L, 100L, 200L);
        List<Long> maxSeqs = List.of(99L, 199L, 250L);
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "mv-target", 0, "source-idx", "uuid-1", 0,
            250L, 1L, 10L,
            files, sizes, minSeqs, maxSeqs
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertEquals(files, deserialized.parquetFiles());
        assertEquals(sizes, deserialized.fileSizes());
        assertEquals(minSeqs, deserialized.fileMinSeqNos());
        assertEquals(maxSeqs, deserialized.fileMaxSeqNos());
    }

    public void testRequestLegacyCompatNoSeqRanges() throws IOException {
        // Using the 10-arg constructor (legacy) — seq ranges should default to -1
        MVCheckpointPublishAction.Request original = new MVCheckpointPublishAction.Request(
            "target", 0, "source", "uuid", 0,
            100L, 1L, 5L,
            List.of("a.parquet", "b.parquet"),
            List.of(1024L, 2048L)
        );

        assertEquals(List.of(-1L, -1L), original.fileMinSeqNos());
        assertEquals(List.of(-1L, -1L), original.fileMaxSeqNos());

        // Round-trip should preserve the -1 defaults
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointPublishAction.Request deserialized = new MVCheckpointPublishAction.Request(in);

        assertEquals(List.of(-1L, -1L), deserialized.fileMinSeqNos());
        assertEquals(List.of(-1L, -1L), deserialized.fileMaxSeqNos());
    }
}
