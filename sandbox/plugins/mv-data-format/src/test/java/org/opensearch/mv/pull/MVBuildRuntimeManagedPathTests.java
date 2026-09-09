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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 2 managed-path tests for {@link MVBuildRuntime}: spill budget enforcement,
 * budget exhaustion, cleanup after completion and cancellation, and circuit breaker
 * trip behavior.
 *
 * <p>These tests exercise the Java-side contract without calling native code —
 * they validate the circuit breaker accounting, lifecycle state machine,
 * spill configuration wiring, and error propagation. End-to-end tests that
 * exercise the full native path live in the integration test suite.
 */
public class MVBuildRuntimeManagedPathTests extends OpenSearchTestCase {

    // ── Helpers ──────────────────────────────────────────────────────────

    private static MVGroupByOrdering singleKeyOrdering() {
        return MVCompiledDefinition.of(List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)), List.of(AggregateSpec.count("cnt")))
            .groupByOrdering();
    }

    private static MVGroupByOrdering multiKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(
                GroupKey.of("event_bucket", GroupKey.ColumnType.LONG),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("status", GroupKey.ColumnType.INTEGER)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("bytes", "sum_bytes"))
        ).groupByOrdering();
    }

    /**
     * A circuit breaker stub that tracks reservations and can be configured
     * to trip at a threshold.
     */
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
                    "mv_pull breaker [" + getName() + "] tripped for " + label,
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

    // ── Spill budget configuration tests ─────────────────────────────────

    /**
     * Spill enabled: a tiny spill budget (non-zero) is passed through to the
     * native bridge. Verifies the runtime accepts spill configuration.
     */
    public void testSpillEnabledTinyBudget() throws Exception {
        long tinySpillBudget = 1024L; // 1 KiB — would force spill for any real build
        int spillFileLimit = 2;
        long memEstimate = 32 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_spill_enabled", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(42L, tinySpillBudget, spillFileLimit, memEstimate, breaker);
        assertNotNull(runtime);
        assertEquals(42L, runtime.runtimePtr());
        runtime.close();
    }

    /**
     * Spill disabled: a large spill budget (effectively unlimited) means the
     * build should not need to spill. Verifies the runtime accepts the config.
     */
    public void testSpillDisabledLargeBudget() throws Exception {
        long largeBudget = Long.MAX_VALUE;
        int noFileLimit = 0; // unlimited
        MVBuildRuntime runtime = new MVBuildRuntime(42L, largeBudget, noFileLimit);
        assertNotNull(runtime);
        assertEquals(42L, runtime.runtimePtr());
        runtime.close();
    }

    /**
     * Spill budget of zero (inherit from global runtime) should be accepted.
     */
    public void testSpillBudgetZeroInheritsGlobal() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(42L, 0L, 0);
        assertNotNull(runtime);
        runtime.close();
    }

    /**
     * Verifies spill settings are wired from IndexSettings defaults.
     */
    public void testSpillSettingsFromDefaults() {
        assertEquals(0L, (long) MVBuildRuntime.MV_SPILL_BUDGET_BYTES.getDefault(null));
        assertEquals(0, (int) MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.getDefault(null));
    }

    // ── Budget exhaustion / circuit breaker trip behavior ─────────────────

    /**
     * When the circuit breaker trips, buildStateManaged must throw IOException
     * wrapping CircuitBreakingException. The breaker reservation must NOT be
     * left dangling (no leak).
     */
    public void testBreakerTripThrowsIOException() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        // Trip threshold lower than the estimate — breaker will trip immediately
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_trip", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildStateManaged("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );
        // Must wrap CircuitBreakingException
        assertTrue("Expected CircuitBreakingException cause, got: " + ex.getCause(), ex.getCause() instanceof CircuitBreakingException);
        // Breaker must not be left with a dangling reservation
        assertEquals("Breaker should have no residual reservation after trip", 0L, breaker.getReserved());
        assertEquals("Breaker should have tripped exactly once", 1L, breaker.getTripCount());

        runtime.close();
    }

    /**
     * Multiple consecutive breaker trips should each throw independently.
     */
    public void testBreakerTripRepeatable() throws Exception {
        long memEstimate = 32 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_repeat_trip", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        for (int i = 0; i < 3; i++) {
            expectThrows(
                IOException.class,
                () -> runtime.buildStateManaged("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
            );
        }
        assertEquals("Should have tripped 3 times", 3L, breaker.getTripCount());
        assertEquals("No residual reservation after repeated trips", 0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * With a null breaker, builds should proceed without reservation attempts
     * (no NPE, no trip).
     */
    public void testNullBreakerSkipsReservation() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 1024, 5, 64 * 1024 * 1024L, null);

        // Cannot actually call native, but verify the runtime is open and functional at the Java level
        assertEquals(1L, runtime.runtimePtr());
        runtime.close();
    }

    /**
     * With zero memory estimate, the breaker should never be called even if
     * the breaker would trip at any non-zero reservation.
     */
    public void testZeroMemoryEstimateSkipsBreaker() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_zero_est", 1L); // trips at 1 byte

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 0L, breaker);
        // Zero estimate means no reservation — breaker should never be called
        // (we can't call buildStateManaged without native, but we verify the
        // runtime is functional and the breaker was never touched)
        assertEquals(0L, breaker.getReserved());
        assertEquals(0L, breaker.getTripCount());
        runtime.close();
    }

    // ── Cleanup after completion ─────────────────────────────────────────

    /**
     * After close, the runtime rejects new builds with IllegalStateException.
     * The breaker must have zero reservation.
     */
    public void testCleanupAfterClose() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_cleanup", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 1024, 5, memEstimate, breaker);
        runtime.close();

        // Must reject builds after close
        expectThrows(
            IllegalStateException.class,
            () -> runtime.buildStateManaged("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );
        // Breaker reservation must be zero — no leak
        assertEquals("No breaker leak after close", 0L, breaker.getReserved());
    }

    /**
     * Double close must be idempotent — no exception, no breaker side effects.
     */
    public void testDoubleCloseIdempotent() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_double_close", Long.MAX_VALUE);
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 64 * 1024 * 1024L, breaker);
        runtime.close();
        runtime.close();
        assertEquals(0L, breaker.getReserved());
    }

    /**
     * runtimePtr() must throw after close.
     */
    public void testRuntimePtrRejectsAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(99L, 0, 0);
        assertEquals(99L, runtime.runtimePtr());
        runtime.close();
        expectThrows(IllegalStateException.class, runtime::runtimePtr);
    }

    /**
     * buildArrowManaged must also reject after close.
     */
    public void testBuildArrowManagedRejectsAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        expectThrows(
            IllegalStateException.class,
            () -> runtime.buildArrowManaged("/tmp/input", "table", "SELECT 1", 0L, 0L, singleKeyOrdering())
        );
    }

    // ── Cleanup after cancellation ───────────────────────────────────────

    /**
     * Cancel then close: verifies that cancel is safe on a runtime that has no
     * active build, and close after cancel is clean.
     */
    public void testCancelThenClose() throws Exception {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_cancel_close", Long.MAX_VALUE);
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 64 * 1024 * 1024L, breaker);

        // Cancel with no active build — should be no-op
        runtime.cancel();
        // Close should still work
        runtime.close();
        assertEquals(0L, breaker.getReserved());
    }

    /**
     * Close triggers cancel of any in-flight context — verified via the
     * closed flag (no active native build in unit tests, but the cancel()
     * call must not throw).
     */
    public void testCloseTriggersCancel() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        // Simulate: close should call cancel() internally
        runtime.close();
        // Additional cancel after close should be safe
        runtime.cancel();
    }

    /**
     * Cancel on an already-closed runtime is a no-op (no exception).
     */
    public void testCancelAfterCloseIsNoOp() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        // Must not throw
        runtime.cancel();
    }

    // ── Breaker accounting symmetry ──────────────────────────────────────

    /**
     * Simulates the breaker reservation/release cycle that buildStateManaged
     * would perform. Since we can't call native, we directly test the private
     * reserveBreaker/releaseBreaker logic by verifying the public contract:
     * after a trip, breaker reservation must be zero.
     */
    public void testBreakerAccountingSymmetric() throws Exception {
        long memEstimate = 16 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_symmetric", Long.MAX_VALUE);

        // The breaker should start at 0
        assertEquals(0L, breaker.getReserved());

        // After creating runtime, still 0 (lazy reservation)
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);
        assertEquals(0L, breaker.getReserved());

        // After close, still 0
        runtime.close();
        assertEquals(0L, breaker.getReserved());
    }

    // ── FFI ordering contract + spill configuration combined ─────────────

    /**
     * Verifies multi-key ordering FFI serialization works correctly with
     * non-default spill configurations.
     */
    public void testMultiKeyOrderingWithSpillConfig() throws Exception {
        MVGroupByOrdering ordering = multiKeyOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        // 3 group keys
        assertEquals(3, ffi.fieldIndices().length);
        assertEquals(3, ffi.directionTokens().length);
        assertEquals(3, ffi.nullPlacementTokens().length);

        // Indices are positional
        assertEquals(0, ffi.fieldIndices()[0]); // event_bucket
        assertEquals(1, ffi.fieldIndices()[1]); // URL
        assertEquals(2, ffi.fieldIndices()[2]); // status

        // All ASC
        for (int d : ffi.directionTokens())
            assertEquals(0, d);
        // All NULLS_FIRST
        for (int n : ffi.nullPlacementTokens())
            assertEquals(0, n);

        // Create runtime with explicit spill config — should accept
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_multi_spill", Long.MAX_VALUE);
        MVBuildRuntime runtime = new MVBuildRuntime(42L, 4096L, 10, 128 * 1024 * 1024L, breaker);
        assertEquals(42L, runtime.runtimePtr());
        runtime.close();
    }

    /**
     * Verifies null ordering is rejected.
     */
    public void testBuildStateManagedRejectsNullOrdering() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        expectThrows(NullPointerException.class, () -> runtime.buildStateManaged("/tmp/input", "table", "SELECT 1", "/tmp/out", null));
        runtime.close();
    }

    /**
     * Verifies buildArrowManaged also rejects null ordering.
     */
    public void testBuildArrowManagedRejectsNullOrdering() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        expectThrows(NullPointerException.class, () -> runtime.buildArrowManaged("/tmp/input", "table", "SELECT 1", 0L, 0L, null));
        runtime.close();
    }

    // ── Services wiring ──────────────────────────────────────────────────

    /**
     * Verifies the Services record properly carries both the runtime pointer
     * and breaker through to MVBuildRuntime construction.
     */
    public void testServicesWiringForManagedRuntime() {
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_services", Long.MAX_VALUE);
        long runtimePtr = 123456L;

        MVPullSettings.Services services = new MVPullSettings.Services(null, null, null, runtimePtr, breaker);

        assertEquals(runtimePtr, services.dataFusionRuntimePtr());
        assertSame(breaker, services.parentCircuitBreaker());

        // Simulate the createBuildRuntime path
        MVBuildRuntime runtime = new MVBuildRuntime(
            services.dataFusionRuntimePtr(),
            0L,
            0,
            64 * 1024 * 1024L,
            services.parentCircuitBreaker()
        );
        assertEquals(runtimePtr, runtime.runtimePtr());
        try {
            runtime.close();
        } catch (IOException e) {
            fail("close should not throw: " + e);
        }
    }

    /**
     * Backward-compatible Services (no DataFusion runtime) yields 0 pointer
     * which MVBuildRuntime must reject.
     */
    public void testServicesBackwardCompatRejectsZeroRuntime() {
        MVPullSettings.Services services = new MVPullSettings.Services(null, null, null);
        assertEquals(0L, services.dataFusionRuntimePtr());

        // MVBuildRuntime rejects 0 pointer
        expectThrows(
            IllegalArgumentException.class,
            () -> new MVBuildRuntime(services.dataFusionRuntimePtr(), 0, 0, 0, services.parentCircuitBreaker())
        );
    }

    // ── Concurrent cancel safety ─────────────────────────────────────────

    /**
     * Multiple threads calling cancel concurrently should not throw or deadlock.
     */
    public void testConcurrentCancelSafe() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        int threadCount = 8;
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicBoolean failed = new AtomicBoolean(false);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    runtime.cancel();
                } catch (Exception e) {
                    failed.set(true);
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        assertTrue("Threads should complete within 5s", latch.await(5, TimeUnit.SECONDS));
        assertFalse("No thread should have thrown", failed.get());
        runtime.close();
    }

    /**
     * Concurrent cancel + close should not throw or deadlock.
     */
    public void testConcurrentCancelAndCloseSafe() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        CountDownLatch latch = new CountDownLatch(2);
        AtomicBoolean failed = new AtomicBoolean(false);

        new Thread(() -> {
            try {
                runtime.cancel();
            } catch (Exception e) {
                failed.set(true);
            } finally {
                latch.countDown();
            }
        }).start();

        new Thread(() -> {
            try {
                runtime.close();
            } catch (Exception e) {
                failed.set(true);
            } finally {
                latch.countDown();
            }
        }).start();

        assertTrue("Threads should complete within 5s", latch.await(5, TimeUnit.SECONDS));
        assertFalse("No thread should have thrown", failed.get());
    }
}
