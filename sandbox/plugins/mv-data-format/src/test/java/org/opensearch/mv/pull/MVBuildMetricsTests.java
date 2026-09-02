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

/**
 * Stage 5 unit tests for {@link MVBuildMetrics}: spill tracking, breaker
 * accounting, RSS recording, fan-in rounds, build lifecycle, snapshot,
 * and reset.
 */
public class MVBuildMetricsTests extends OpenSearchTestCase {

    private MVBuildMetrics metrics;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metrics = new MVBuildMetrics();
    }

    // ── Spill tracking ───────────────────────────────────────────────────

    public void testSpillRecording() {
        metrics.recordSpill(1024, 2);
        assertEquals(1024L, metrics.getSpillBytes());
        assertEquals(2L, metrics.getSpillFiles());
    }

    public void testSpillAccumulates() {
        metrics.recordSpill(100, 1);
        metrics.recordSpill(200, 3);
        assertEquals(300L, metrics.getSpillBytes());
        assertEquals(4L, metrics.getSpillFiles());
    }

    public void testSpillZeroInitial() {
        assertEquals(0L, metrics.getSpillBytes());
        assertEquals(0L, metrics.getSpillFiles());
    }

    // ── Breaker accounting ───────────────────────────────────────────────

    public void testBreakerReservationAndRelease() {
        long estimate = 64 * 1024 * 1024L;
        metrics.recordBreakerReservation(estimate);
        assertEquals(1L, metrics.getBreakerReservations());
        assertEquals(estimate, metrics.getActiveBreakerBytes());

        metrics.recordBreakerRelease(estimate);
        assertEquals(1L, metrics.getBreakerReservations());
        assertEquals(0L, metrics.getActiveBreakerBytes());
    }

    public void testBreakerTripCounting() {
        metrics.recordBreakerTrip();
        metrics.recordBreakerTrip();
        assertEquals(2L, metrics.getBreakerTrips());
    }

    public void testBreakerReservationSymmetric() {
        long estimate = 32 * 1024 * 1024L;
        for (int i = 0; i < 5; i++) {
            metrics.recordBreakerReservation(estimate);
        }
        for (int i = 0; i < 5; i++) {
            metrics.recordBreakerRelease(estimate);
        }
        assertEquals(5L, metrics.getBreakerReservations());
        assertEquals(0L, metrics.getActiveBreakerBytes());
    }

    // ── RSS recording ────────────────────────────────────────────────────

    public void testPeakRssTracking() {
        metrics.recordRss(100);
        metrics.recordRss(200);
        metrics.recordRss(150);
        assertEquals(200L, metrics.getPeakRssBytes());
    }

    public void testPeakRssNeverDecreases() {
        metrics.recordRss(500);
        metrics.recordRss(100);
        assertEquals(500L, metrics.getPeakRssBytes());
    }

    // ── Fan-in recording ─────────────────────────────────────────────────

    public void testFanInRounds() {
        metrics.recordFanInRound();
        metrics.recordFanInRound();
        metrics.recordFanInRound();
        assertEquals(3L, metrics.getFanInRounds());
    }

    // ── Build lifecycle ──────────────────────────────────────────────────

    public void testBuildCounting() {
        metrics.recordBuildCompleted();
        metrics.recordBuildCompleted();
        metrics.recordBuildFailed();
        assertEquals(3L, metrics.getTotalBuilds());
        assertEquals(1L, metrics.getFailedBuilds());
    }

    // ── Snapshot ─────────────────────────────────────────────────────────

    public void testSnapshotContainsAllMetrics() {
        metrics.recordSpill(512, 1);
        metrics.recordBreakerReservation(1024);
        metrics.recordBreakerTrip();
        metrics.recordRss(9999);
        metrics.recordFanInRound();
        metrics.recordBuildCompleted();

        Map<String, Long> snap = metrics.snapshot();
        assertEquals(Long.valueOf(512), snap.get("spill_bytes"));
        assertEquals(Long.valueOf(1), snap.get("spill_files"));
        assertEquals(Long.valueOf(1), snap.get("breaker_reservations"));
        assertEquals(Long.valueOf(1), snap.get("breaker_trips"));
        assertEquals(Long.valueOf(1024), snap.get("active_breaker_bytes"));
        assertEquals(Long.valueOf(9999), snap.get("peak_rss_bytes"));
        assertEquals(Long.valueOf(1), snap.get("fan_in_rounds"));
        assertEquals(Long.valueOf(1), snap.get("total_builds"));
        assertEquals(Long.valueOf(0), snap.get("failed_builds"));
    }

    public void testSnapshotIsUnmodifiable() {
        Map<String, Long> snap = metrics.snapshot();
        expectThrows(UnsupportedOperationException.class, () -> snap.put("foo", 1L));
    }

    // ── Reset ────────────────────────────────────────────────────────────

    public void testReset() {
        metrics.recordSpill(100, 2);
        metrics.recordBreakerReservation(50);
        metrics.recordBreakerTrip();
        metrics.recordRss(999);
        metrics.recordFanInRound();
        metrics.recordBuildCompleted();
        metrics.recordBuildFailed();

        metrics.reset();

        assertEquals(0L, metrics.getSpillBytes());
        assertEquals(0L, metrics.getSpillFiles());
        assertEquals(0L, metrics.getBreakerReservations());
        assertEquals(0L, metrics.getBreakerTrips());
        assertEquals(0L, metrics.getActiveBreakerBytes());
        assertEquals(0L, metrics.getPeakRssBytes());
        assertEquals(0L, metrics.getFanInRounds());
        assertEquals(0L, metrics.getTotalBuilds());
        assertEquals(0L, metrics.getFailedBuilds());
    }
}
