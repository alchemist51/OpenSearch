/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/** Tests the append-only sequence coverage invariant used before advancing an MV watermark. */
public class MVCoverageIntegrityTests extends OpenSearchTestCase {

    private static long nextWatermark(long current, long observedMax, long advertisedMax, long totalRows) {
        if (observedMax < 0L) {
            return current;
        }
        long appliedThrough = Math.min(observedMax, advertisedMax);
        return MVWatermark.hasCompleteCoverage(current, appliedThrough, totalRows) ? appliedThrough : current;
    }

    public void testCompleteRangeAdvances() {
        assertEquals(200L, nextWatermark(100L, 200L, 200L, 100L));
    }

    public void testHoleBelowObservedMaximumHolds() {
        assertEquals(100L, nextWatermark(100L, 200L, 200L, 76L));
    }

    public void testOverCountAlsoHolds() {
        assertEquals(100L, nextWatermark(100L, 200L, 200L, 105L));
    }

    public void testObservedMaximumBelowAdvertisedMaximumCanAdvance() {
        assertEquals(150L, nextWatermark(100L, 150L, 200L, 50L));
    }

    public void testEmptyDeltaHolds() {
        assertEquals(100L, nextWatermark(100L, -1L, 200L, 0L));
    }

    public void testMissingFirstDocumentHoldsEmptyWatermark() {
        assertEquals(-1L, nextWatermark(-1L, 5L, 5L, 5L));
    }

    public void testCompleteRetryAdvancesExactlyOnce() {
        long watermark = nextWatermark(100L, 200L, 200L, 76L);
        assertEquals(100L, watermark);
        watermark = nextWatermark(watermark, 200L, 200L, 100L);
        assertEquals(200L, watermark);
    }

    public void testCanonicalGapWouldHaveHeldWatermark() {
        assertEquals(99_000_000L, nextWatermark(99_000_000L, 99_997_496L, 99_997_496L, 997_473L));
    }

    public void testDeltaCarriesObservedSourceRowCount() {
        MVDataFusionReadEngine.Delta delta = new MVDataFusionReadEngine.Delta(Map.of(1L, new long[] { 100L, 297L }), 99L, 100L);
        assertEquals(100L, delta.totalRows());
        assertEquals(99L, delta.observedMaxSeqNo());
    }
}
