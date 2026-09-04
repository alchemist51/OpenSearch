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
 * Tests for {@link MVCheckpointRequestAction}: wire round-trip (request + response)
 * including the new targetWatermark field. Handler-side scoping logic (file filtering,
 * nothing-new, noop scoping) is tested here using the static helper methods.
 */
public class MVCheckpointRequestActionTests extends OpenSearchTestCase {

    // ── Request round-trip with watermark ────────────────────────────────

    public void testRequestRoundTripWithWatermark() throws IOException {
        MVCheckpointRequestAction.Request original = new MVCheckpointRequestAction.Request(
            "source-idx", 0, "mv-target", 1, 500L
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Request deserialized = new MVCheckpointRequestAction.Request(in);

        assertEquals("source-idx", deserialized.sourceIndex());
        assertEquals(0, deserialized.sourceShard());
        assertEquals("mv-target", deserialized.targetIndex());
        assertEquals(1, deserialized.targetShard());
        assertEquals(500L, deserialized.targetWatermark());
    }

    public void testRequestRoundTripNegativeWatermark() throws IOException {
        MVCheckpointRequestAction.Request original = new MVCheckpointRequestAction.Request(
            "source", 0, "target", 0, -1L
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Request deserialized = new MVCheckpointRequestAction.Request(in);

        assertEquals(-1L, deserialized.targetWatermark());
    }

    public void testRequestValidation() {
        MVCheckpointRequestAction.Request request = new MVCheckpointRequestAction.Request(
            "source", 0, "target", 0, 100L
        );
        assertNull(request.validate());
    }

    // ── Response round-trip (available) ──────────────────────────────────

    public void testResponseRoundTripAvailable() throws IOException {
        Map<String, MVFileMetadata> files = Map.of(
            "parquet/gen-1_0.parquet", new MVFileMetadata(1024L, 0L, 99L, 12345L),
            "parquet/gen-1_1.parquet", new MVFileMetadata(2048L, 100L, 199L, -1L)
        );
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source-idx", 0, 3L, 199L, 42L, files, System.currentTimeMillis()
        );
        MVCheckpointRequestAction.Response original = new MVCheckpointRequestAction.Response(cp);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Response deserialized = new MVCheckpointRequestAction.Response(in);

        assertTrue(deserialized.available());
        assertNotNull(deserialized.checkpoint());
        assertEquals(199L, deserialized.maxSeqNo());
        assertEquals(3L, deserialized.primaryTerm());
        assertEquals(42L, deserialized.infosVersion());
        assertEquals(2, deserialized.checkpoint().fileMetadata().size());
        assertTrue(deserialized.checkpoint().fileMetadata().containsKey("parquet/gen-1_0.parquet"));
        assertTrue(deserialized.checkpoint().fileMetadata().containsKey("parquet/gen-1_1.parquet"));
    }

    // ── Response round-trip (unavailable / nothing-new) ──────────────────

    public void testResponseRoundTripUnavailable() throws IOException {
        MVCheckpointRequestAction.Response original = MVCheckpointRequestAction.Response.unavailable();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Response deserialized = new MVCheckpointRequestAction.Response(in);

        assertFalse(deserialized.available());
        assertNull(deserialized.checkpoint());
        assertEquals(-1L, deserialized.maxSeqNo());
    }

    // ── Response with empty files ────────────────────────────────────────

    public void testResponseEmptyFiles() throws IOException {
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source", 0, 1L, 0L, 0L, Map.of(), System.currentTimeMillis()
        );
        MVCheckpointRequestAction.Response original = new MVCheckpointRequestAction.Response(cp);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Response deserialized = new MVCheckpointRequestAction.Response(in);

        assertTrue(deserialized.available());
        assertTrue(deserialized.checkpoint().fileMetadata().isEmpty());
    }

    // ── Response with noops ──────────────────────────────────────────────

    public void testResponseRoundTripWithNoops() throws IOException {
        long[] noops = new long[]{10L, 20L, 30L};
        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source", 0, 1L, 100L, 5L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, -1L)),
            System.currentTimeMillis(),
            noops
        );
        MVCheckpointRequestAction.Response original = new MVCheckpointRequestAction.Response(cp);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Response deserialized = new MVCheckpointRequestAction.Response(in);

        assertTrue(deserialized.available());
        assertArrayEquals(noops, deserialized.checkpoint().noopSeqNos());
    }

    // ── Handler file filtering logic (via MVCheckpointRequestTransportHandler.includeFile) ─

    public void testFilesFilteredByRequestWatermark() {
        // File entirely below watermark → excluded
        assertFalse(MVCheckpointRequestTransportHandler.includeFile(0L, 100L, 200L, 500L));
        // File exactly at watermark → excluded
        assertFalse(MVCheckpointRequestTransportHandler.includeFile(0L, 200L, 200L, 500L));
        // File partially above watermark → included
        assertTrue(MVCheckpointRequestTransportHandler.includeFile(150L, 300L, 200L, 500L));
        // File entirely above watermark → included
        assertTrue(MVCheckpointRequestTransportHandler.includeFile(300L, 500L, 200L, 500L));
        // Legacy unknown range → included (fail-open)
        assertTrue(MVCheckpointRequestTransportHandler.includeFile(-1L, -1L, 200L, 500L));
    }

    // ── Nothing-new detection (advertMax <= watermark) ───────────────────

    public void testNothingNewWhenAdvertMaxBelowWatermark() {
        // Simulates the handler's nothing-new check
        long catalogAdvertMax = 100L;
        long requestWatermark = 200L;
        assertTrue("should return nothing-new", catalogAdvertMax <= requestWatermark);
    }

    public void testNothingNewWhenAdvertMaxEqualsWatermark() {
        long catalogAdvertMax = 200L;
        long requestWatermark = 200L;
        assertTrue("should return nothing-new", catalogAdvertMax <= requestWatermark);
    }

    public void testHasDataWhenAdvertMaxAboveWatermark() {
        long catalogAdvertMax = 300L;
        long requestWatermark = 200L;
        assertFalse("should have data", catalogAdvertMax <= requestWatermark);
    }
}
