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

    // ── Remote metadata cache counters (Fix B observability) ─────────────
    private final AtomicLong metadataCacheHits = new AtomicLong(0);
    private final AtomicLong metadataCacheRefreshes = new AtomicLong(0);
    private final AtomicLong incrementalSyncSkippedFiles = new AtomicLong(0);

    // ── Compaction counters ──────────────────────────────────────────────
    private final AtomicLong compactionsStarted = new AtomicLong(0);

    // ── Checkpoint consume latency (push-to-consume observability) ──────
    private final AtomicLong checkpointConsumeLatencyLast = new AtomicLong(0);
    private final AtomicLong checkpointConsumeLatencyMax = new AtomicLong(0);

    // ── Checksum counters (Stage 3: O(1) checksum for mv_state) ──────────
    private final AtomicLong checksumRegistered = new AtomicLong(0);
    private final AtomicLong checksumMisses = new AtomicLong(0);
    private final AtomicLong compactionsCompleted = new AtomicLong(0);
    private final AtomicLong compactionsFailed = new AtomicLong(0);
    private final AtomicLong compactionsSkipped = new AtomicLong(0);
    private final AtomicLong compactionInputGenerations = new AtomicLong(0);
    private final AtomicLong compactionInputBytes = new AtomicLong(0);
    private final AtomicLong compactionOutputRows = new AtomicLong(0);
    private final AtomicLong compactionOutputBytes = new AtomicLong(0);
    private final AtomicLong compactionDurationMs = new AtomicLong(0);

    // ── CRC verification counters (downloadFiles post-download check) ───
    private final AtomicLong crcVerifyPassed = new AtomicLong(0);
    private final AtomicLong crcVerifyFailed = new AtomicLong(0);

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

    // ── Metadata cache recording ─────────────────────────────────────────

    public void recordMetadataCacheHit() {
        metadataCacheHits.incrementAndGet();
    }

    public void recordMetadataCacheRefresh() {
        metadataCacheRefreshes.incrementAndGet();
    }

    public void recordIncrementalSyncSkippedFiles(long count) {
        incrementalSyncSkippedFiles.addAndGet(count);
    }

    public long getMetadataCacheHits() {
        return metadataCacheHits.get();
    }

    public long getMetadataCacheRefreshes() {
        return metadataCacheRefreshes.get();
    }

    public long getIncrementalSyncSkippedFiles() {
        return incrementalSyncSkippedFiles.get();
    }

    // ── Compaction recording ─────────────────────────────────────────────

    // ── Checkpoint consume latency recording ────────────────────────────

    public void recordCheckpointConsumeLatency(long latencyMs) {
        checkpointConsumeLatencyLast.set(latencyMs);
        checkpointConsumeLatencyMax.accumulateAndGet(latencyMs, Math::max);
    }

    public long getCheckpointConsumeLatencyLast() {
        return checkpointConsumeLatencyLast.get();
    }

    public long getCheckpointConsumeLatencyMax() {
        return checkpointConsumeLatencyMax.get();
    }

    // ── Checksum recording (Stage 3: O(1) checksum for mv_state) ─────────

    public void recordChecksumRegistered() {
        checksumRegistered.incrementAndGet();
    }

    public void recordChecksumMiss() {
        checksumMisses.incrementAndGet();
    }

    public long getChecksumRegistered() {
        return checksumRegistered.get();
    }

    public long getChecksumMisses() {
        return checksumMisses.get();
    }

    // ── Compaction recording ─────────────────────────────────────────────

    public void recordCompactionStarted() {
        compactionsStarted.incrementAndGet();
    }

    public void recordCompactionCompleted(int inputGens, long inputBytes, long outputRows, long outputBytes, long durationMs) {
        compactionsCompleted.incrementAndGet();
        compactionInputGenerations.addAndGet(inputGens);
        compactionInputBytes.addAndGet(inputBytes);
        compactionOutputRows.addAndGet(outputRows);
        compactionOutputBytes.addAndGet(outputBytes);
        compactionDurationMs.addAndGet(durationMs);
    }

    public void recordCompactionFailed() {
        compactionsFailed.incrementAndGet();
    }

    public void recordCompactionSkipped() {
        compactionsSkipped.incrementAndGet();
    }

    public long getCompactionsStarted() {
        return compactionsStarted.get();
    }

    public long getCompactionsCompleted() {
        return compactionsCompleted.get();
    }

    public long getCompactionsFailed() {
        return compactionsFailed.get();
    }

    public long getCompactionsSkipped() {
        return compactionsSkipped.get();
    }

    // ── CRC verification recording ──────────────────────────────────────

    public void recordCrcVerifyPassed() {
        crcVerifyPassed.incrementAndGet();
    }

    public void recordCrcVerifyFailed() {
        crcVerifyFailed.incrementAndGet();
    }

    public long getCrcVerifyPassed() {
        return crcVerifyPassed.get();
    }

    public long getCrcVerifyFailed() {
        return crcVerifyFailed.get();
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
        m.put("metadata_cache_hits", metadataCacheHits.get());
        m.put("metadata_cache_refreshes", metadataCacheRefreshes.get());
        m.put("incremental_sync_skipped_files", incrementalSyncSkippedFiles.get());
        m.put("compactions_started", compactionsStarted.get());
        m.put("compactions_completed", compactionsCompleted.get());
        m.put("compactions_failed", compactionsFailed.get());
        m.put("compactions_skipped", compactionsSkipped.get());
        m.put("compaction_input_generations", compactionInputGenerations.get());
        m.put("compaction_input_bytes", compactionInputBytes.get());
        m.put("compaction_output_rows", compactionOutputRows.get());
        m.put("compaction_output_bytes", compactionOutputBytes.get());
        m.put("compaction_duration_ms", compactionDurationMs.get());
        m.put("checksum_registered", checksumRegistered.get());
        m.put("checksum_misses", checksumMisses.get());
        m.put("checkpoint_consume_latency_last_ms", checkpointConsumeLatencyLast.get());
        m.put("checkpoint_consume_latency_max_ms", checkpointConsumeLatencyMax.get());
        m.put("crc_verify_passed", crcVerifyPassed.get());
        m.put("crc_verify_failed", crcVerifyFailed.get());
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
        metadataCacheHits.set(0);
        metadataCacheRefreshes.set(0);
        incrementalSyncSkippedFiles.set(0);
        compactionsStarted.set(0);
        compactionsCompleted.set(0);
        compactionsFailed.set(0);
        compactionsSkipped.set(0);
        compactionInputGenerations.set(0);
        compactionInputBytes.set(0);
        compactionOutputRows.set(0);
        compactionOutputBytes.set(0);
        compactionDurationMs.set(0);
        checksumRegistered.set(0);
        checksumMisses.set(0);
        checkpointConsumeLatencyLast.set(0);
        checkpointConsumeLatencyMax.set(0);
        crcVerifyPassed.set(0);
        crcVerifyFailed.set(0);
    }
}
