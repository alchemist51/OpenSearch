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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.mv.pull.MVWatermark;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Tests for Defect 13: noop accounting in the MV checkpoint flow.
 *
 * <ul>
 *   <li>MVNoopTracker: record, range query, eviction, cap enforcement</li>
 *   <li>MVNoopIndexingListener: failure index + delete recording, non-recording cases</li>
 *   <li>MVReplicationCheckpoint: wire round-trip with noops (delta encoding)</li>
 *   <li>Coverage arithmetic: MVWatermark.hasCompleteCoverage with noops</li>
 *   <li>MVDerivedArtifactBuilder.countNoopsInRange: binary search correctness</li>
 * </ul>
 */
public class MVNoopAccountingTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("source-idx", "uuid-1"), 0);

    // ── MVNoopTracker: recording ─────────────────────────────────────────

    public void testTrackerRecordsFailureAndDeleteSeqNos() {
        MVNoopTracker tracker = new MVNoopTracker();

        tracker.recordNoop(SHARD, 5L);
        tracker.recordNoop(SHARD, 10L);
        tracker.recordNoop(SHARD, 15L);

        assertEquals(3, tracker.trackedCount(SHARD));
        assertEquals(3L, tracker.totalRecorded());
    }

    public void testTrackerIgnoresNegativeSeqNo() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, -1L);
        tracker.recordNoop(SHARD, -2L); // UNASSIGNED_SEQ_NO
        assertEquals(0, tracker.trackedCount(SHARD));
    }

    public void testTrackerIdempotentRecording() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, 10L);
        tracker.recordNoop(SHARD, 10L); // duplicate
        assertEquals(1, tracker.trackedCount(SHARD));
        assertEquals(2L, tracker.totalRecorded()); // recorded count increments even for dups
    }

    // ── MVNoopTracker: range queries ─────────────────────────────────────

    public void testGetNoopsInRangeReturnsCorrectSubset() {
        MVNoopTracker tracker = new MVNoopTracker();
        for (long seq : new long[]{2, 5, 10, 15, 20, 25, 30}) {
            tracker.recordNoop(SHARD, seq);
        }

        // Range (5, 20] should return {10, 15, 20}
        long[] result = tracker.getNoopsInRange(SHARD, 5L, 20L);
        assertArrayEquals(new long[]{10L, 15L, 20L}, result);
    }

    public void testGetNoopsInRangeEmptyForNoShard() {
        MVNoopTracker tracker = new MVNoopTracker();
        long[] result = tracker.getNoopsInRange(SHARD, 0L, 100L);
        assertEquals(0, result.length);
    }

    public void testGetNoopsInRangeFromNegativeOne() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, 0L);
        tracker.recordNoop(SHARD, 1L);
        tracker.recordNoop(SHARD, 5L);

        // Range (-1, 5] should return all: {0, 1, 5}
        long[] result = tracker.getNoopsInRange(SHARD, -1L, 5L);
        assertArrayEquals(new long[]{0L, 1L, 5L}, result);
    }

    public void testGetNoopsInRangeEmptyWhenNoneInRange() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, 100L);
        tracker.recordNoop(SHARD, 200L);

        long[] result = tracker.getNoopsInRange(SHARD, 0L, 50L);
        assertEquals(0, result.length);
    }

    // ── MVNoopTracker: eviction ──────────────────────────────────────────

    public void testEvictBelowMinTargetWatermark() {
        MVNoopTracker tracker = new MVNoopTracker();
        for (long seq : new long[]{5, 10, 15, 20, 25}) {
            tracker.recordNoop(SHARD, seq);
        }

        int evicted = tracker.evictBelow(SHARD, 15L);
        assertEquals(3, evicted); // evicts 5, 10, 15
        assertEquals(2, tracker.trackedCount(SHARD)); // 20, 25 remain
        assertEquals(3L, tracker.totalEvicted());

        // Verify remaining
        long[] remaining = tracker.getNoopsInRange(SHARD, -1L, 100L);
        assertArrayEquals(new long[]{20L, 25L}, remaining);
    }

    public void testEvictBelowNegativeWatermarkIsNoop() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, 5L);
        int evicted = tracker.evictBelow(SHARD, -1L);
        assertEquals(0, evicted);
        assertEquals(1, tracker.trackedCount(SHARD));
    }

    // ── MVNoopTracker: cap enforcement ───────────────────────────────────

    public void testCapEvictsOldestEntries() {
        int cap = 5;
        MVNoopTracker tracker = new MVNoopTracker(cap);

        for (long seq = 0; seq < 8; seq++) {
            tracker.recordNoop(SHARD, seq);
        }

        // Cap is 5, so 3 oldest should be evicted (0, 1, 2)
        assertEquals(5, tracker.trackedCount(SHARD));
        assertEquals(3L, tracker.capEvictions());

        // Remaining should be 3, 4, 5, 6, 7
        long[] remaining = tracker.getNoopsInRange(SHARD, -1L, 100L);
        assertArrayEquals(new long[]{3L, 4L, 5L, 6L, 7L}, remaining);
    }

    // ── MVNoopTracker: shard removal ─────────────────────────────────────

    public void testRemoveShardCleansUp() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD, 5L);
        tracker.recordNoop(SHARD, 10L);
        assertEquals(1, tracker.trackedShardCount());

        tracker.removeShard(SHARD);
        assertEquals(0, tracker.trackedShardCount());
        assertEquals(0, tracker.trackedCount(SHARD));
    }

    // ── MVNoopIndexingListener: recording via listener ───────────────────

    public void testListenerRecordsFailedIndexWithAssignedSeqNo() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Simulate a mapping parse failure on primary: FAILURE with assigned seqNo
        Engine.IndexResult failedResult = new Engine.IndexResult(
            new IllegalArgumentException("mapping parse failure"), 1L, 1L, 42L
        );
        listener.postIndex(SHARD, null, failedResult);

        assertEquals(1, tracker.trackedCount(SHARD));
        long[] noops = tracker.getNoopsInRange(SHARD, -1L, 100L);
        assertArrayEquals(new long[]{42L}, noops);
    }

    public void testListenerIgnoresSuccessfulIndex() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        Engine.IndexResult successResult = new Engine.IndexResult(1L, 1L, 42L, true);
        listener.postIndex(SHARD, null, successResult);

        assertEquals(0, tracker.trackedCount(SHARD));
    }

    public void testListenerIgnoresFailedIndexWithUnassignedSeqNo() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Pre-engine failure: UNASSIGNED_SEQ_NO (-2)
        Engine.IndexResult preEngineFailure = new Engine.IndexResult(
            new IllegalArgumentException("pre-engine"), 1L
        );
        listener.postIndex(SHARD, null, preEngineFailure);

        assertEquals(0, tracker.trackedCount(SHARD));
    }

    public void testListenerRecordsSuccessfulDelete() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Successful delete — found=true, has assigned seqNo
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(1L, 1L, 55L, true);
        listener.postDelete(SHARD, null, deleteResult);

        assertEquals(1, tracker.trackedCount(SHARD));
        long[] noops = tracker.getNoopsInRange(SHARD, -1L, 100L);
        assertArrayEquals(new long[]{55L}, noops);
    }

    public void testListenerRecordsDeleteNotFound() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Delete of non-existent doc — found=false, still has seqNo
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(1L, 1L, 60L, false);
        listener.postDelete(SHARD, null, deleteResult);

        assertEquals(1, tracker.trackedCount(SHARD));
    }

    public void testListenerRecordsFailedDelete() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Failed delete with assigned seqNo
        Engine.DeleteResult failedDelete = new Engine.DeleteResult(
            new IllegalArgumentException("version conflict"), 1L, 1L, 70L, false
        );
        listener.postDelete(SHARD, null, failedDelete);

        assertEquals(1, tracker.trackedCount(SHARD));
    }

    public void testListenerIgnoresDeleteWithUnassignedSeqNo() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVNoopIndexingListener listener = new MVNoopIndexingListener(tracker);

        // Pre-engine delete failure
        Engine.DeleteResult preEngine = new Engine.DeleteResult(
            new IllegalArgumentException("pre"), 1L, 1L
        );
        // This constructor sets seqNo to UNASSIGNED_SEQ_NO (-2)
        listener.postDelete(SHARD, null, preEngine);

        assertEquals(0, tracker.trackedCount(SHARD));
    }

    // ── MVReplicationCheckpoint: wire round-trip with noops ──────────────

    public void testCheckpointRoundTripWithNoops() throws IOException {
        long[] noops = new long[]{5L, 10L, 15L, 100L, 999L};
        MVReplicationCheckpoint original = new MVReplicationCheckpoint(
            "source-idx", 0, 3L, 999L, 42L,
            Map.of("gen-1.parquet", new MVFileMetadata(1024L, 0L, 999L, -1L)),
            1700000000000L,
            noops
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVReplicationCheckpoint deserialized = new MVReplicationCheckpoint(in);

        assertEquals(999L, deserialized.maxSeqNo());
        assertArrayEquals(noops, deserialized.noopSeqNos());
        assertEquals(5, deserialized.noopSeqNos().length);
    }

    public void testCheckpointRoundTripWithEmptyNoops() throws IOException {
        MVReplicationCheckpoint original = new MVReplicationCheckpoint(
            "source", 0, 1L, 100L, 5L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, -1L)),
            System.currentTimeMillis()
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVReplicationCheckpoint deserialized = new MVReplicationCheckpoint(in);

        assertEquals(0, deserialized.noopSeqNos().length);
    }

    public void testCheckpointRoundTripWithSingleNoop() throws IOException {
        long[] noops = new long[]{42L};
        MVReplicationCheckpoint original = new MVReplicationCheckpoint(
            "source", 0, 1L, 100L, 5L,
            Map.of(), System.currentTimeMillis(), noops
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        MVReplicationCheckpoint deserialized = new MVReplicationCheckpoint(in);

        assertArrayEquals(new long[]{42L}, deserialized.noopSeqNos());
    }

    public void testCheckpointEmptySentinelHasNoNoops() {
        MVReplicationCheckpoint empty = MVReplicationCheckpoint.empty("source", 0);
        assertEquals(0, empty.noopSeqNos().length);
    }

    // ── Coverage arithmetic: MVWatermark.hasCompleteCoverage with noops ──

    public void testCoverageWithZeroNoops() {
        // range (10, 20] = 10 rows expected, 10 found → pass
        assertTrue(MVWatermark.hasCompleteCoverage(10L, 20L, 10L, 0));
    }

    public void testCoverageWithNoopsReducesExpected() {
        // range (10, 20] = 10 seqNos, 3 noops → 7 rows expected, 7 found → pass
        assertTrue(MVWatermark.hasCompleteCoverage(10L, 20L, 7L, 3));
    }

    public void testCoverageFailsOnShortfallEvenWithNoops() {
        // range (10, 20] = 10 seqNos, 3 noops → 7 expected, only 5 found → fail
        assertFalse(MVWatermark.hasCompleteCoverage(10L, 20L, 5L, 3));
    }

    public void testCoverageFailsOnOvercountWithNoops() {
        // range (10, 20] = 10 seqNos, 2 noops → 8 expected, 9 found → fail
        assertFalse(MVWatermark.hasCompleteCoverage(10L, 20L, 9L, 2));
    }

    public void testCoverageBackwardCompatWithoutNoops() {
        // Original signature still works
        assertTrue(MVWatermark.hasCompleteCoverage(10L, 20L, 10L));
        assertFalse(MVWatermark.hasCompleteCoverage(10L, 20L, 9L));
    }

    // ── MVWatermark.countNoopsInRange ───────────────────────────────

    public void testCountNoopsInRangeBasic() {
        long[] noops = new long[]{5L, 10L, 15L, 20L, 25L};

        // Range (5, 20] → {10, 15, 20} = 3
        assertEquals(3, MVWatermark.countNoopsInRange(noops, 5L, 20L));
    }

    public void testCountNoopsInRangeEmpty() {
        assertEquals(0, MVWatermark.countNoopsInRange(new long[0], 0L, 100L));
        assertEquals(0, MVWatermark.countNoopsInRange(null, 0L, 100L));
    }

    public void testCountNoopsInRangeNoMatch() {
        long[] noops = new long[]{100L, 200L};
        assertEquals(0, MVWatermark.countNoopsInRange(noops, 0L, 50L));
    }

    public void testCountNoopsInRangeAllMatch() {
        long[] noops = new long[]{1L, 2L, 3L};
        assertEquals(3, MVWatermark.countNoopsInRange(noops, 0L, 3L));
    }

    public void testCountNoopsInRangeExcludesLowerBound() {
        long[] noops = new long[]{10L, 20L, 30L};
        // Range (10, 30] → {20, 30} = 2 (10 excluded)
        assertEquals(2, MVWatermark.countNoopsInRange(noops, 10L, 30L));
    }

    public void testCountNoopsInRangeIncludesUpperBound() {
        long[] noops = new long[]{10L, 20L, 30L};
        // Range (0, 20] → {10, 20} = 2 (20 included)
        assertEquals(2, MVWatermark.countNoopsInRange(noops, 0L, 20L));
    }

}
