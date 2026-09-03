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
 * Wire round-trip tests for {@link MVCheckpointRequestAction.Request} and
 * {@link MVCheckpointRequestAction.Response}.
 */
public class MVCheckpointRequestActionTests extends OpenSearchTestCase {

    // ── Request round-trip ───────────────────────────────────────────────

    public void testRequestRoundTrip() throws IOException {
        MVCheckpointRequestAction.Request original = new MVCheckpointRequestAction.Request(
            "source-idx", 0, "mv-target", 1
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVCheckpointRequestAction.Request deserialized = new MVCheckpointRequestAction.Request(in);

        assertEquals("source-idx", deserialized.sourceIndex());
        assertEquals(0, deserialized.sourceShard());
        assertEquals("mv-target", deserialized.targetIndex());
        assertEquals(1, deserialized.targetShard());
    }

    public void testRequestValidation() {
        MVCheckpointRequestAction.Request request = new MVCheckpointRequestAction.Request(
            "source", 0, "target", 0
        );
        assertNull(request.validate());
    }

    // ── Response round-trip (available) ──────────────────────────────────

    public void testResponseRoundTripAvailable() throws IOException {
        Map<String, MVFileMetadata> files = Map.of(
            "gen-1_0.parquet", new MVFileMetadata(1024L, 0L, 99L, 12345L),
            "gen-1_1.parquet", new MVFileMetadata(2048L, 100L, 199L, -1L)
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
    }

    // ── Response round-trip (unavailable) ────────────────────────────────

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
}
