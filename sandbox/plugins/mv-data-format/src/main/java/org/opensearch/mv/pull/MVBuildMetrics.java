/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 5: Thread-safe metrics accumulator for MV pull builds.
 * Exposed via the plugin's stats endpoint / JMX for observability.
 *
 * <p>All counters are monotonically increasing (reset only on node restart).
 * Gauges (peak_rss, active_breaker_bytes) reflect current state.
 */
public final class MVBuildMetrics {

    /** Singleton node-level instance. */
    public static final MVBuildMetrics INSTANCE = new MVBuildMetrics();

    // ── Spill counters (monotonic) ───────────────────────────────────────
    private final AtomicLong spillBytes = new AtomicLong(0);
    private final AtomicLong spillFiles = new AtomicLong(0);

    // ── Breaker counters ─────────────────────────────────────────────────
    private final AtomicLong breakerReservations = new AtomicLong(0);
    private final AtomicLong breakerTrips = new AtomicLong(0);
    private final AtomicLong activeBreakerBytes = new AtomicLong(0);

    // ── RSS / memory gauge ───────────────────────────────────────────────
    private final AtomicLong peakRssBytes = new AtomicLong(0);

    // ── Fan-in counter ───────────────────────────────────────────────────
    private final AtomicLong fanInRounds = new AtomicLong(0);

    // ── Build counter ────────────────────────────────────────────────────
    private final AtomicLong totalBuilds = new AtomicLong(0);
    private final AtomicLong failedBuilds = new AtomicLong(0);

    // ── Build duration ───────────────────────────────────────────────────
    private final AtomicLong totalBuildDurationUs = new AtomicLong(0);

    // ── Output batch counter ─────────────────────────────────────────────
    private final AtomicLong totalOutputBatches = new AtomicLong(0);

    MVBuildMetrics() {}

    // ── Spill recording ──────────────────────────────────────────────────

    public void recordSpill(long bytes, int files) {
        spillBytes.addAndGet(bytes);
        spillFiles.addAndGet(files);
    }

    public long getSpillBytes() {
        return spillBytes.get();
    }

    public long getSpillFiles() {
        return spillFiles.get();
    }

    // ── Breaker recording ────────────────────────────────────────────────

    public void recordBreakerReservation(long bytes) {
        breakerReservations.incrementAndGet();
        activeBreakerBytes.addAndGet(bytes);
    }

    public void recordBreakerRelease(long bytes) {
        activeBreakerBytes.addAndGet(-bytes);
    }

    public void recordBreakerTrip() {
        breakerTrips.incrementAndGet();
    }

    public long getBreakerReservations() {
        return breakerReservations.get();
    }

    public long getBreakerTrips() {
        return breakerTrips.get();
    }

    public long getActiveBreakerBytes() {
        return activeBreakerBytes.get();
    }

    // ── RSS recording ────────────────────────────────────────────────────

    public void recordRss(long rssBytes) {
        peakRssBytes.updateAndGet(current -> Math.max(current, rssBytes));
    }

    public long getPeakRssBytes() {
        return peakRssBytes.get();
    }

    // ── Fan-in recording ─────────────────────────────────────────────────

    public void recordFanInRound() {
        fanInRounds.incrementAndGet();
    }

    public long getFanInRounds() {
        return fanInRounds.get();
    }

    // ── Build recording ──────────────────────────────────────────────────

    public void recordBuildCompleted() {
        totalBuilds.incrementAndGet();
    }

    public void recordBuildFailed() {
        failedBuilds.incrementAndGet();
        totalBuilds.incrementAndGet();
    }

    public long getTotalBuilds() {
        return totalBuilds.get();
    }

    public long getFailedBuilds() {
        return failedBuilds.get();
    }

    // ── Build duration recording ─────────────────────────────────────────

    public void recordBuildDuration(long us) {
        totalBuildDurationUs.addAndGet(us);
    }

    public long getTotalBuildDurationUs() {
        return totalBuildDurationUs.get();
    }

    // ── Output batch recording ───────────────────────────────────────────

    public void recordOutputBatches(int count) {
        totalOutputBatches.addAndGet(count);
    }

    public long getTotalOutputBatches() {
        return totalOutputBatches.get();
    }

    /**
     * Returns a snapshot of all metrics as a map suitable for
     * stats endpoint serialization.
     */
    public Map<String, Long> snapshot() {
        Map<String, Long> m = new LinkedHashMap<>();
        m.put("spill_bytes", spillBytes.get());
        m.put("spill_files", spillFiles.get());
        m.put("breaker_reservations", breakerReservations.get());
        m.put("breaker_trips", breakerTrips.get());
        m.put("active_breaker_bytes", activeBreakerBytes.get());
        m.put("peak_rss_bytes", peakRssBytes.get());
        m.put("fan_in_rounds", fanInRounds.get());
        m.put("total_builds", totalBuilds.get());
        m.put("failed_builds", failedBuilds.get());
        m.put("total_build_duration_us", totalBuildDurationUs.get());
        m.put("total_output_batches", totalOutputBatches.get());
        return Collections.unmodifiableMap(m);
    }

    /** Reset all counters (for testing only). */
    void reset() {
        spillBytes.set(0);
        spillFiles.set(0);
        breakerReservations.set(0);
        breakerTrips.set(0);
        activeBreakerBytes.set(0);
        peakRssBytes.set(0);
        fanInRounds.set(0);
        totalBuilds.set(0);
        failedBuilds.set(0);
        totalBuildDurationUs.set(0);
        totalOutputBatches.set(0);
    }
}
