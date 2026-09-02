/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.mv.AggregateSpec;
import org.opensearch.mv.GroupKey;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVGroupByOrdering;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link MVBuildRuntime} and the Stage 2 FFI ordering contract,
 * circuit breaker integration, spill budget configuration, cancellation, and
 * lifecycle management.
 */
public class MVBuildRuntimeTests extends OpenSearchTestCase {

    // ── FFI ordering contract serialization ──────────────────────────────

    public void testOrderingFFISerializationSingleKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();

        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);
        assertEquals(1, ffi.fieldIndices().length);
        assertEquals(0, ffi.fieldIndices()[0]);
        assertEquals(0, ffi.directionTokens()[0]); // ASC
        assertEquals(0, ffi.nullPlacementTokens()[0]); // NULLS_FIRST
    }

    public void testOrderingFFISerializationMultiKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.LONG),
                GroupKey.of("k2", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("m", "sum_m"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();

        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);
        assertEquals(3, ffi.fieldIndices().length);
        // Keys must be in definition order: k0=0, k1=1, k2=2
        assertEquals(0, ffi.fieldIndices()[0]);
        assertEquals(1, ffi.fieldIndices()[1]);
        assertEquals(2, ffi.fieldIndices()[2]);
        // All ASC
        for (int dir : ffi.directionTokens()) {
            assertEquals(0, dir);
        }
        // All NULLS_FIRST
        for (int np : ffi.nullPlacementTokens()) {
            assertEquals(0, np);
        }
    }

    public void testOrderingFFIEmptyKeys() {
        MVBuildRuntime.MVOrderingFFI ffi = new MVBuildRuntime.MVOrderingFFI(new int[0], new int[0], new int[0]);
        assertEquals(0, ffi.fieldIndices().length);
    }

    public void testOrderingFFIParallelArraysMatchKeyOrder() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("event_bucket", GroupKey.ColumnType.LONG), GroupKey.of("URL", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        // Verify parallel arrays maintain key-to-index mapping
        assertEquals(2, ffi.fieldIndices().length);
        assertEquals(2, ffi.directionTokens().length);
        assertEquals(2, ffi.nullPlacementTokens().length);

        // event_bucket = state field index 0, URL = state field index 1
        assertEquals(0, ffi.fieldIndices()[0]);
        assertEquals(1, ffi.fieldIndices()[1]);
    }

    // ── MVBuildRuntime lifecycle ─────────────────────────────────────────

    public void testBuildRuntimeRejectsZeroPointer() {
        expectThrows(IllegalArgumentException.class, () -> new MVBuildRuntime(0, 0, 0));
    }

    public void testBuildRuntimeRejectsZeroPointerWithBreaker() {
        expectThrows(IllegalArgumentException.class, () -> new MVBuildRuntime(0, 0, 0, 0, new NoopCircuitBreaker("test")));
    }

    public void testBuildRuntimeCloseIdempotent() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        runtime.close(); // second close should be no-op
    }

    public void testBuildRuntimeRejectsAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        expectThrows(
            IllegalStateException.class,
            () -> runtime.buildStateManaged(
                "/tmp/input",
                "table",
                "SELECT 1",
                "/tmp/out",
                MVCompiledDefinition.of(List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)), List.of(AggregateSpec.count("cnt")))
                    .groupByOrdering()
            )
        );
    }

    public void testBuildRuntimePtrAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        assertEquals(1L, runtime.runtimePtr());
        runtime.close();
        expectThrows(IllegalStateException.class, runtime::runtimePtr);
    }

    // ── Cancellation (Java side, no native calls) ────────────────────────

    public void testCancelOnClosedRuntimeIsNoOp() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        // Should not throw — cancel is a no-op when contextId is 0
        runtime.cancel();
    }

    public void testCancelBeforeAnyBuild() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        // No active build — cancel should be no-op
        runtime.cancel();
        runtime.close();
    }

    // ── Wire token stability ─────────────────────────────────────────────

    public void testWireTokenValues() {
        assertEquals(0, MVGroupByOrdering.Direction.ASCENDING.wireToken());
        assertEquals(0, MVGroupByOrdering.NullPlacement.NULLS_FIRST.wireToken());
        assertEquals(1, MVGroupByOrdering.NullPlacement.NULLS_LAST.wireToken());
    }

    // ── Circuit breaker integration ──────────────────────────────────────

    public void testBuildRuntimeCreatesWithBreaker() throws Exception {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test_mv");
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 1024 * 1024, 10, 64 * 1024 * 1024, breaker);
        assertNotNull(runtime);
        assertEquals(1L, runtime.runtimePtr());
        runtime.close();
    }

    public void testBuildRuntimeCreatesWithNullBreaker() throws Exception {
        // Null breaker should be accepted — no circuit breaker accounting
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 0, null);
        assertNotNull(runtime);
        runtime.close();
    }

    public void testBuildRuntimeWithZeroMemoryEstimate() throws Exception {
        // Zero estimate means no breaker reservation
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test_mv");
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 0, breaker);
        assertNotNull(runtime);
        runtime.close();
    }

    // ── Spill budget configuration ───────────────────────────────────────

    public void testSpillBudgetBytesSettingDefault() {
        assertEquals(0L, (long) MVBuildRuntime.MV_SPILL_BUDGET_BYTES.getDefault(null));
    }

    public void testSpillFileCountLimitSettingDefault() {
        assertEquals(0, (int) MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.getDefault(null));
    }

    public void testBuildMemoryEstimateSettingDefault() {
        assertEquals(64L * 1024 * 1024, (long) MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.getDefault(null));
    }

    // ── MVPullSettings.Services backward compatibility ───────────────────

    public void testServicesBackwardCompatibleConstructor() {
        MVPullSettings.Services services = new MVPullSettings.Services(null, null, null);
        assertEquals(0L, services.dataFusionRuntimePtr());
        assertNull(services.parentCircuitBreaker());
    }

    public void testServicesFullConstructor() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker("test");
        MVPullSettings.Services services = new MVPullSettings.Services(null, null, null, 42L, breaker);
        assertEquals(42L, services.dataFusionRuntimePtr());
        assertSame(breaker, services.parentCircuitBreaker());
    }

    // ── Ordering contract: roundtrip invariant ───────────────────────────

    public void testOrderingFFIRoundtripPreservesContract() {
        // The FFI serialization must preserve the exact contract from groupByOrdering()
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("a", GroupKey.ColumnType.LONG),
                GroupKey.of("b", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("c", GroupKey.ColumnType.LONG),
                GroupKey.of("d", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        // Verify: every key maps to stateFieldIndex == its position
        for (int i = 0; i < ordering.size(); i++) {
            assertEquals("stateFieldIndex[" + i + "]", ordering.keys().get(i).stateFieldIndex(), ffi.fieldIndices()[i]);
            assertEquals("direction[" + i + "]", ordering.keys().get(i).direction().wireToken(), ffi.directionTokens()[i]);
            assertEquals("nullPlacement[" + i + "]", ordering.keys().get(i).nullPlacement().wireToken(), ffi.nullPlacementTokens()[i]);
        }
    }
}
