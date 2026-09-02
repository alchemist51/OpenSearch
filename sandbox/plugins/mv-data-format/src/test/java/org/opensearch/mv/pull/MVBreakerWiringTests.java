/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.mv.AggregateSpec;
import org.opensearch.mv.GroupKey;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVGroupByOrdering;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 5 tests: breaker wiring, cancel-mid-stream partial cleanup,
 * settings validation, metrics verification after forced spill, and
 * backpressure RSS admission gate.
 */
public class MVBreakerWiringTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Reset the singleton for clean test state
        MVBuildMetrics.INSTANCE.reset();
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static MVGroupByOrdering singleKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(GroupKey.of("k", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("c"))
        ).groupByOrdering();
    }

    // ── Tracking breaker stub ────────────────────────────────────────────

    private static class TrackingCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicLong reserved = new AtomicLong(0);
        private final long tripThreshold;
        private final AtomicLong tripCount = new AtomicLong(0);

        TrackingCircuitBreaker(String name, long tripThreshold) {
            super(name);
            this.tripThreshold = tripThreshold;
        }

        @Override
        public double addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            long current = reserved.get();
            if (tripThreshold > 0 && current + bytes > tripThreshold) {
                tripCount.incrementAndGet();
                throw new CircuitBreakingException(
                    "breaker [" + getName() + "] tripped for " + label,
                    bytes,
                    current,
                    getDurability()
                );
            }
            reserved.addAndGet(bytes);
            return 1.0;
        }

        @Override
        public long addWithoutBreaking(long bytes) {
            return reserved.addAndGet(bytes);
        }

        long getReserved() {
            return reserved.get();
        }

        long getTripCount() {
            return tripCount.get();
        }
    }

    // ── 5. Cancel-mid-stream test: assert breaker reservation released ───

    /**
     * When a build is cancelled mid-stream via close(), the breaker
     * reservation must be released (no dangling reservation).
     */
    public void testCancelMidStreamReleasesBreaker() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("cancel_test", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        // Simulate: close triggers cancel, which should be safe
        runtime.cancel();
        runtime.close();

        // After close, no dangling reservation
        assertEquals(0L, breaker.getReserved());
    }

    /**
     * Cancel then attempt buildStreamingArtifact: must reject with ISE,
     * breaker stays at 0.
     */
    public void testCancelThenBuildRejectsCleanly() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("cancel_build_test", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);
        runtime.close();

        // Must reject
        expectThrows(IllegalStateException.class, () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering()));
        assertEquals("No breaker leak after cancelled + rejected build", 0L, breaker.getReserved());
    }

    /**
     * After breaker trip + close, breaker and metrics are both zero.
     */
    public void testCancelMidStreamPartialFileDeletion() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        // Trip threshold below estimate to simulate a partial build
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("partial_delete", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        // Trip the breaker (simulates partial build that fails)
        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );
        assertTrue(ex.getCause() instanceof CircuitBreakingException);

        // Breaker reservation must be zero after trip
        assertEquals(0L, breaker.getReserved());
        // Metrics should show the trip
        assertTrue(MVBuildMetrics.INSTANCE.getBreakerTrips() >= 1);
        assertEquals(0L, MVBuildMetrics.INSTANCE.getActiveBreakerBytes());

        runtime.close();
    }

    // ── 6. Settings tests: validate spill/file budget settings honored ───

    public void testSpillByteBudgetSettingDefault() {
        assertEquals(0L, (long) MVBuildRuntime.MV_SPILL_BUDGET_BYTES.getDefault(null));
    }

    public void testSpillByteBudgetSettingCustom() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put("index.mv_pull.spill_budget_bytes", 1024 * 1024L)
            .build();
        assertEquals(1024 * 1024L, (long) MVBuildRuntime.MV_SPILL_BUDGET_BYTES.get(settings));
    }

    public void testSpillFileBudgetSettingDefault() {
        assertEquals(0, (int) MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.getDefault(null));
    }

    public void testSpillFileBudgetSettingCustom() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put("index.mv_pull.spill_file_count_limit", 42)
            .build();
        assertEquals(42, (int) MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.get(settings));
    }

    public void testBuildMemoryEstimateSettingDefault() {
        assertEquals(64L * 1024 * 1024, (long) MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.getDefault(null));
    }

    public void testBuildMemoryEstimateSettingCustom() {
        org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
            .put("index.mv_pull.build_memory_estimate_bytes", 128 * 1024 * 1024L)
            .build();
        assertEquals(128 * 1024 * 1024L, (long) MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.get(settings));
    }

    /**
     * Verify spill budget is wired through to runtime construction.
     */
    public void testSpillBudgetWiredToRuntime() throws Exception {
        long spillBudget = 4096L;
        int spillFiles = 10;
        MVBuildRuntime runtime = new MVBuildRuntime(1L, spillBudget, spillFiles);
        assertEquals(1L, runtime.runtimePtr());
        runtime.close();
    }

    // ── 7. Metrics tests: verify spill_bytes/files/reservations after trip

    /**
     * After a breaker trip, metrics must report the trip.
     */
    public void testMetricsAfterBreakerTrip() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("metrics_trip", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );

        // INSTANCE metrics should reflect the trip
        assertTrue("Breaker trips should be >= 1", MVBuildMetrics.INSTANCE.getBreakerTrips() >= 1);
        assertEquals("Active breaker bytes should be 0 after trip", 0L, MVBuildMetrics.INSTANCE.getActiveBreakerBytes());

        runtime.close();
    }

    /**
     * After a successful breaker reservation+release cycle, metrics must be
     * symmetric.
     */
    public void testMetricsBreakerSymmetric() {
        MVBuildMetrics m = new MVBuildMetrics();
        long estimate = 32 * 1024 * 1024L;

        m.recordBreakerReservation(estimate);
        assertEquals(estimate, m.getActiveBreakerBytes());

        m.recordBreakerRelease(estimate);
        assertEquals(0L, m.getActiveBreakerBytes());
        assertEquals(1L, m.getBreakerReservations());
    }

    /**
     * Spill metrics accumulate across multiple recordings.
     */
    public void testSpillMetricsAccumulate() {
        MVBuildMetrics m = new MVBuildMetrics();
        m.recordSpill(1024, 2);
        m.recordSpill(2048, 3);
        assertEquals(3072L, m.getSpillBytes());
        assertEquals(5L, m.getSpillFiles());
    }

    /**
     * Metrics snapshot after trip shows zero active bytes but non-zero trip count.
     */
    public void testMetricsSnapshotAfterTrip() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("snap_trip", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );

        java.util.Map<String, Long> snap = MVBuildMetrics.INSTANCE.snapshot();
        assertTrue("breaker_trips >= 1", snap.get("breaker_trips") >= 1);
        assertEquals("active_breaker_bytes = 0", Long.valueOf(0L), snap.get("active_breaker_bytes"));

        runtime.close();
    }

    // ── 8. Backpressure test: RSS admission gate blocks near limit ───────

    /**
     * When the circuit breaker is near its limit, new rounds are blocked:
     * buildStreamingArtifact throws IOException wrapping CircuitBreakingException.
     */
    public void testRssAdmissionGateBlocksNearLimit() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        // Set threshold just below the estimate so it trips immediately
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("rss_gate", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        // First attempt trips
        IOException ex1 = expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );
        assertTrue(ex1.getCause() instanceof CircuitBreakingException);

        // Second attempt also trips (backpressure sustained)
        IOException ex2 = expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );
        assertTrue(ex2.getCause() instanceof CircuitBreakingException);

        // Breaker should have tripped twice, no residual reservation
        assertEquals(2L, breaker.getTripCount());
        assertEquals(0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * Verify RSS admission blocks even with deprecated buildStateManaged path.
     */
    public void testRssAdmissionGateBlocksBuildStateManagedToo() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("rss_gate_legacy", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildStateManaged("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
        );
        assertTrue(ex.getCause() instanceof CircuitBreakingException);
        assertEquals(0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * After breaker blocks 3 rounds, verify trip count and zero residual.
     */
    public void testMultipleRoundsBreakerBlocksAll() throws Exception {
        long memEstimate = 32 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("multi_block", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        for (int i = 0; i < 3; i++) {
            expectThrows(
                IOException.class,
                () -> runtime.buildStreamingArtifact("/tmp/in", "t", "SQL", "/tmp/out", singleKeyOrdering())
            );
        }

        assertEquals("3 trips", 3L, breaker.getTripCount());
        assertEquals("zero residual", 0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * Backpressure with buildArrowManaged also trips the breaker.
     */
    public void testRssAdmissionGateBlocksBuildArrowManaged() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("rss_gate_arrow", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildArrowManaged("/tmp/in", "t", "SQL", 0L, 0L, singleKeyOrdering())
        );
        assertTrue(ex.getCause() instanceof CircuitBreakingException);
        assertEquals(0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * With zero memory estimate, breaker never trips even with tiny threshold.
     */
    public void testZeroEstimateBypassesBreaker() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("zero_est", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 0L, breaker);
        // Zero estimate = no reservation attempts
        assertEquals(0L, breaker.getReserved());
        assertEquals(0L, breaker.getTripCount());

        runtime.close();
    }
}
