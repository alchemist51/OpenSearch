/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Unit tests for {@link MVCheckpointPublisher}'s source-side file filtering
 * and per-target watermark CAS logic.
 */
public class MVCheckpointPublisherTests extends OpenSearchTestCase {

    // ── includeFile tests ────────────────────────────────────────────────

    public void testIncludeFile_LegacyUnknownMaxSeqNo() {
        // Legacy file (maxSeqNo == -1): always included regardless of watermark
        assertTrue(MVCheckpointPublisher.includeFile(-1L, -1L, 500L, 1000L));
    }

    public void testIncludeFile_FileEntirelyBelowWatermark() {
        // File [0, 100] and watermark is 200 → excluded
        assertFalse(MVCheckpointPublisher.includeFile(0L, 100L, 200L, 1000L));
    }

    public void testIncludeFile_FileExactlyAtWatermark() {
        // File [0, 200] and watermark is 200 → excluded (maxSeqNo <= watermark)
        assertFalse(MVCheckpointPublisher.includeFile(0L, 200L, 200L, 1000L));
    }

    public void testIncludeFile_FilePartiallyAboveWatermark() {
        // File [150, 300] and watermark is 200 → included (data above watermark)
        assertTrue(MVCheckpointPublisher.includeFile(150L, 300L, 200L, 1000L));
    }

    public void testIncludeFile_FileEntirelyAboveWatermark() {
        // File [500, 700] and watermark is 200 → included
        assertTrue(MVCheckpointPublisher.includeFile(500L, 700L, 200L, 1000L));
    }

    public void testIncludeFile_WatermarkIsZero() {
        // Watermark 0, file [0, 0] → excluded (at boundary)
        assertFalse(MVCheckpointPublisher.includeFile(0L, 0L, 0L, 100L));
        // Watermark 0, file [0, 1] → included (has data above watermark)
        assertTrue(MVCheckpointPublisher.includeFile(0L, 1L, 0L, 100L));
    }

    public void testIncludeFile_SingleDocFile() {
        // File covers exactly one doc [50, 50]
        // Watermark at 49 → included
        assertTrue(MVCheckpointPublisher.includeFile(50L, 50L, 49L, 100L));
        // Watermark at 50 → excluded
        assertFalse(MVCheckpointPublisher.includeFile(50L, 50L, 50L, 100L));
    }

    public void testIncludeFile_WatermarkMinusOne_FailOpen() {
        // This case is handled by the caller (unknown watermark sends full list)
        // but the function itself: watermark=-1, file [0,100] → maxSeqNo(100) > -1 → included
        assertTrue(MVCheckpointPublisher.includeFile(0L, 100L, -1L, 1000L));
    }

    // ── updateTargetWatermark CAS-max tests ──────────────────────────────

    public void testUpdateTargetWatermark_NewKey() {
        MVCheckpointPublisher publisher = createPublisher();
        publisher.updateTargetWatermark("target:0", 100L);
        assertEquals(100L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_Advance() {
        MVCheckpointPublisher publisher = createPublisher();
        publisher.updateTargetWatermark("target:0", 100L);
        publisher.updateTargetWatermark("target:0", 200L);
        assertEquals(200L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_NoRegression() {
        MVCheckpointPublisher publisher = createPublisher();
        publisher.updateTargetWatermark("target:0", 200L);
        publisher.updateTargetWatermark("target:0", 100L); // older watermark
        assertEquals(200L, publisher.getLastKnownWatermark("target:0")); // no regression
    }

    public void testUpdateTargetWatermark_NegativeIgnored() {
        MVCheckpointPublisher publisher = createPublisher();
        publisher.updateTargetWatermark("target:0", -1L);
        assertEquals(-1L, publisher.getLastKnownWatermark("target:0")); // not stored
    }

    public void testUpdateTargetWatermark_PerShardIndependence() {
        MVCheckpointPublisher publisher = createPublisher();
        publisher.updateTargetWatermark("target-a:0", 100L);
        publisher.updateTargetWatermark("target-b:0", 500L);
        assertEquals(100L, publisher.getLastKnownWatermark("target-a:0"));
        assertEquals(500L, publisher.getLastKnownWatermark("target-b:0"));
        assertEquals(-1L, publisher.getLastKnownWatermark("unknown:0")); // never set
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private MVCheckpointPublisher createPublisher() {
        // Create a minimal publisher for testing watermark logic.
        // Client and routingService are not used in the watermark/filter tests.
        NodeRoutingSnapshotService fakeRouting = new NodeRoutingSnapshotService("test-node") {
            @Override
            public java.util.Map<String, java.util.List<BoundTarget>> sourceToTargets() {
                return java.util.Map.of();
            }
        };
        return new MVCheckpointPublisher(null, "source-idx", "uuid-1", 0, fakeRouting);
    }
}
