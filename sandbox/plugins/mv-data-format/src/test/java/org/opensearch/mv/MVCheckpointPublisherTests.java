/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MVCheckpointPublisher}'s source-side file filtering,
 * per-target watermark CAS logic, per-target publish skip, and anyTargetBehind.
 */
public class MVCheckpointPublisherTests extends OpenSearchTestCase {

    // ── includeFile tests ────────────────────────────────────────────────

    public void testIncludeFile_LegacyUnknownMaxSeqNo() {
        assertTrue(MVCheckpointPublisher.includeFile(-1L, -1L, 500L, 1000L));
    }

    public void testIncludeFile_FileEntirelyBelowWatermark() {
        assertFalse(MVCheckpointPublisher.includeFile(0L, 100L, 200L, 1000L));
    }

    public void testIncludeFile_FileExactlyAtWatermark() {
        assertFalse(MVCheckpointPublisher.includeFile(0L, 200L, 200L, 1000L));
    }

    public void testIncludeFile_FilePartiallyAboveWatermark() {
        assertTrue(MVCheckpointPublisher.includeFile(150L, 300L, 200L, 1000L));
    }

    public void testIncludeFile_FileEntirelyAboveWatermark() {
        assertTrue(MVCheckpointPublisher.includeFile(500L, 700L, 200L, 1000L));
    }

    public void testIncludeFile_WatermarkIsZero() {
        assertFalse(MVCheckpointPublisher.includeFile(0L, 0L, 0L, 100L));
        assertTrue(MVCheckpointPublisher.includeFile(0L, 1L, 0L, 100L));
    }

    public void testIncludeFile_SingleDocFile() {
        assertTrue(MVCheckpointPublisher.includeFile(50L, 50L, 49L, 100L));
        assertFalse(MVCheckpointPublisher.includeFile(50L, 50L, 50L, 100L));
    }

    public void testIncludeFile_WatermarkMinusOne_FailOpen() {
        assertTrue(MVCheckpointPublisher.includeFile(0L, 100L, -1L, 1000L));
    }

    // ── updateTargetWatermark CAS-max tests ──────────────────────────────

    public void testUpdateTargetWatermark_NewKey() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        publisher.updateTargetWatermark("target:0", 100L);
        assertEquals(100L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_Advance() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        publisher.updateTargetWatermark("target:0", 100L);
        publisher.updateTargetWatermark("target:0", 200L);
        assertEquals(200L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_NoRegression() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        publisher.updateTargetWatermark("target:0", 200L);
        publisher.updateTargetWatermark("target:0", 100L);
        assertEquals(200L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_NegativeIgnored() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        publisher.updateTargetWatermark("target:0", -1L);
        assertEquals(-1L, publisher.getLastKnownWatermark("target:0"));
    }

    public void testUpdateTargetWatermark_PerShardIndependence() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        publisher.updateTargetWatermark("target-a:0", 100L);
        publisher.updateTargetWatermark("target-b:0", 500L);
        assertEquals(100L, publisher.getLastKnownWatermark("target-a:0"));
        assertEquals(500L, publisher.getLastKnownWatermark("target-b:0"));
        assertEquals(-1L, publisher.getLastKnownWatermark("unknown:0"));
    }

    // ── anyTargetBehind tests ────────────────────────────────────────────

    public void testAnyTargetBehind_AllUnknown() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1")
            )
        ));
        // No watermark responses yet → unknown (-1) → behind
        assertTrue(publisher.anyTargetBehind(100L));
    }

    public void testAnyTargetBehind_AllAtParity() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1")
            )
        ));
        publisher.updateTargetWatermark("mv-target:0", 100L);
        assertFalse(publisher.anyTargetBehind(100L));
    }

    public void testAnyTargetBehind_OneOfTwoBehind() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target-a", 1, "uuid-1"),
                new NodeRoutingSnapshotService.BoundTarget("mv-target-b", 1, "uuid-1")
            )
        ));
        publisher.updateTargetWatermark("mv-target-a:0", 100L);
        // mv-target-b has no watermark → behind
        assertTrue(publisher.anyTargetBehind(100L));
    }

    public void testAnyTargetBehind_NegativeAdvertMax() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1")
            )
        ));
        assertFalse(publisher.anyTargetBehind(-1L));
    }

    public void testAnyTargetBehind_NoTargets() {
        MVCheckpointPublisher publisher = createPublisher(Map.of());
        assertFalse(publisher.anyTargetBehind(100L));
    }

    // ── minTargetWatermark tests ─────────────────────────────────────────

    public void testMinTargetWatermark_OneTarget() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1")
            )
        ));
        publisher.updateTargetWatermark("mv-target:0", 200L);
        assertEquals(200L, publisher.minTargetWatermark());
    }

    public void testMinTargetWatermark_TwoTargets_ReturnsMin() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target-a", 1, "uuid-1"),
                new NodeRoutingSnapshotService.BoundTarget("mv-target-b", 1, "uuid-1")
            )
        ));
        publisher.updateTargetWatermark("mv-target-a:0", 100L);
        publisher.updateTargetWatermark("mv-target-b:0", 300L);
        assertEquals(100L, publisher.minTargetWatermark());
    }

    public void testMinTargetWatermark_UnknownTargetReturnsMinus1() {
        MVCheckpointPublisher publisher = createPublisher(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target-a", 1, "uuid-1"),
                new NodeRoutingSnapshotService.BoundTarget("mv-target-b", 1, "uuid-1")
            )
        ));
        publisher.updateTargetWatermark("mv-target-a:0", 100L);
        // mv-target-b unknown → returns -1
        assertEquals(-1L, publisher.minTargetWatermark());
    }

    // ── targetSkipCount metric ───────────────────────────────────────────

    public void testTargetSkipCountIncremented() {
        // Create a publisher with a real routing setup but null client
        // (publish won't actually send, but the skip path will fire)
        FakeRoutingService routing = new FakeRoutingService(Map.of(
            "source-idx", List.of(
                new NodeRoutingSnapshotService.BoundTarget("mv-target", 1, "uuid-1")
            )
        ));
        MVCheckpointPublisher publisher = new MVCheckpointPublisher(null, "source-idx", "uuid-1", 0, routing);
        // Set watermark at parity
        publisher.updateTargetWatermark("mv-target:0", 100L);

        MVReplicationCheckpoint cp = new MVReplicationCheckpoint(
            "source-idx", 0, 1L, 100L, 5L, Map.of(), System.currentTimeMillis()
        );

        // Should skip the target since watermark >= maxSeqNo
        int sent = publisher.publish(cp);
        assertEquals(0, sent);
        assertEquals(1L, publisher.targetSkipCount());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    /** Simple routing that returns a fixed source→targets map. */
    static class FakeRoutingService extends NodeRoutingSnapshotService {
        private final Map<String, List<BoundTarget>> srcToTgt;

        FakeRoutingService(Map<String, List<BoundTarget>> srcToTgt) {
            super("test-node");
            this.srcToTgt = srcToTgt;
        }

        @Override
        public Map<String, List<BoundTarget>> sourceToTargets() {
            return srcToTgt;
        }
    }

    private MVCheckpointPublisher createPublisher(Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt) {
        FakeRoutingService fakeRouting = new FakeRoutingService(srcToTgt);
        return new MVCheckpointPublisher(null, "source-idx", "uuid-1", 0, fakeRouting);
    }
}
