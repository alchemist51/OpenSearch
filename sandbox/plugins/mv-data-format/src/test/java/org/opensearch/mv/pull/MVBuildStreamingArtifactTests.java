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
 * Stage 3 unit tests for the streaming artifact build path on
 * {@link MVBuildRuntime}. These tests validate the Java-side contract for
 * {@code buildStreamingArtifact}, {@code ArtifactResult} validation,
 * FFI serialization of multi-key orderings with mixed column types,
 * schema hash determinism, and circuit breaker accounting.
 *
 * <p>Since {@code buildStreamingArtifact} may initially delegate to
 * {@code buildStateManaged} (with a TODO for native streaming wiring),
 * these tests exercise the Java contract without requiring native code.
 * End-to-end tests with the full native path live in the integration
 * test suite.
 */
public class MVBuildStreamingArtifactTests extends OpenSearchTestCase {

    // ── Helpers ──────────────────────────────────────────────────────────

    private static MVGroupByOrdering singleKeyOrdering() {
        return MVCompiledDefinition.of(List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)), List.of(AggregateSpec.count("cnt")))
            .groupByOrdering();
    }

    private static MVGroupByOrdering threeKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(
                GroupKey.of("event_bucket", GroupKey.ColumnType.LONG),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("bytes", "sum_bytes"))
        ).groupByOrdering();
    }

    private static MVGroupByOrdering fiveKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(
                GroupKey.of("EventTime", GroupKey.ColumnType.LONG),
                GroupKey.of("RegionID", GroupKey.ColumnType.INTEGER),
                GroupKey.of("OS", GroupKey.ColumnType.LONG),
                GroupKey.of("CounterID", GroupKey.ColumnType.INTEGER),
                GroupKey.of("IsRefresh", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();
    }

    private static MVCompiledDefinition singleKeyDefinition() {
        return MVCompiledDefinition.of(List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)), List.of(AggregateSpec.count("cnt")));
    }

    /**
     * Circuit breaker stub that tracks reservations and can trip at a
     * configured threshold. Same pattern as MVBuildRuntimeManagedPathTests.
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

    // ── 1. ArtifactResult validation ─────────────────────────────────────

    /**
     * ArtifactResult must reject non-positive row counts.
     */
    public void testArtifactResultRejectsZeroRowCount() {
        expectThrows(IllegalArgumentException.class, () -> new MVBuildRuntime.ArtifactResult(0, "abc123", "def456"));
    }

    public void testArtifactResultRejectsNegativeRowCount() {
        expectThrows(IllegalArgumentException.class, () -> new MVBuildRuntime.ArtifactResult(-1, "abc123", "def456"));
    }

    /**
     * ArtifactResult must reject null hashes.
     */
    public void testArtifactResultRejectsNullSchemaHash() {
        expectThrows(NullPointerException.class, () -> new MVBuildRuntime.ArtifactResult(100, null, "def456"));
    }

    public void testArtifactResultRejectsNullDefinitionHash() {
        expectThrows(NullPointerException.class, () -> new MVBuildRuntime.ArtifactResult(100, "abc123", null));
    }

    /**
     * ArtifactResult with valid inputs should construct successfully.
     */
    public void testArtifactResultValidConstruction() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(42, "deadbeef", "cafebabe");
        assertEquals(42, result.rowCount());
        // Backwards-compat constructor stores 0L for hash fields (strings not persisted)
        assertEquals(0L, result.schemaHash());
        assertEquals(0L, result.definitionHash());
        assertTrue(result.isOk());
    }

    // ── 2. buildStreamingArtifact rejects null ordering ──────────────────

    /**
     * Null ordering must be rejected with NullPointerException, matching
     * the existing testBuildStateManagedRejectsNullOrdering pattern.
     */
    public void testBuildStreamingArtifactRejectsNullOrdering() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        expectThrows(NullPointerException.class, () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", null));
        runtime.close();
    }

    // ── 3. buildStreamingArtifact rejects after close ────────────────────

    /**
     * After close, buildStreamingArtifact must throw IllegalStateException,
     * matching the existing testBuildRuntimeRejectsAfterClose pattern.
     */
    public void testBuildStreamingArtifactRejectsAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        expectThrows(
            IllegalStateException.class,
            () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );
    }

    // ── 4. Circuit breaker trip on buildStreamingArtifact ─────────────────

    /**
     * When the circuit breaker trips, buildStreamingArtifact must throw
     * IOException wrapping CircuitBreakingException, and the breaker
     * reservation must not be left dangling.
     */
    public void testBreakerTripOnBuildStreamingArtifact() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        // Trip threshold lower than estimate — breaker trips immediately
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_streaming_trip", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );
        assertTrue("Expected CircuitBreakingException cause, got: " + ex.getCause(), ex.getCause() instanceof CircuitBreakingException);
        assertEquals("Breaker should have no residual reservation after trip", 0L, breaker.getReserved());
        assertEquals("Breaker should have tripped exactly once", 1L, breaker.getTripCount());

        runtime.close();
    }

    // ── 5. Multiple group-by keys (3+) FFI serialization ─────────────────

    /**
     * Verify that 3-key orderings serialize correctly through MVOrderingFFI:
     * all three field indices, direction tokens, and null placement tokens
     * must be present and in definition order.
     */
    public void testThreeKeyFFISerialization() {
        MVGroupByOrdering ordering = threeKeyOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(3, ffi.fieldIndices().length);
        assertEquals(3, ffi.directionTokens().length);
        assertEquals(3, ffi.nullPlacementTokens().length);

        // event_bucket=0, URL=1, UserID=2
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

    /**
     * Verify that 5-key orderings serialize correctly with all indices
     * preserved in order.
     */
    public void testFiveKeyFFISerialization() {
        MVGroupByOrdering ordering = fiveKeyOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(5, ffi.fieldIndices().length);
        for (int i = 0; i < 5; i++) {
            assertEquals("field index " + i, i, ffi.fieldIndices()[i]);
            assertEquals("direction " + i, 0, ffi.directionTokens()[i]);
            assertEquals("nullPlacement " + i, 0, ffi.nullPlacementTokens()[i]);
        }
    }

    // ── 6. Various integer width group keys ──────────────────────────────

    /**
     * Verify LONG type group keys produce correct ordering FFI.
     */
    public void testLongTypeGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("a", GroupKey.ColumnType.LONG), GroupKey.of("b", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(2, ffi.fieldIndices().length);
        assertEquals(0, ffi.fieldIndices()[0]);
        assertEquals(1, ffi.fieldIndices()[1]);
    }

    /**
     * Verify INTEGER type group keys produce correct ordering FFI.
     */
    public void testIntegerTypeGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("a", GroupKey.ColumnType.INTEGER), GroupKey.of("b", GroupKey.ColumnType.INTEGER)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(2, ffi.fieldIndices().length);
        assertEquals(0, ffi.fieldIndices()[0]);
        assertEquals(1, ffi.fieldIndices()[1]);
    }

    /**
     * Verify mixed LONG + INTEGER + KEYWORD group keys serialize correctly.
     * The FFI only carries field indices and sort semantics — column type
     * does not affect the ordering contract.
     */
    public void testMixedColumnTypeGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("ts", GroupKey.ColumnType.LONG),
                GroupKey.of("region", GroupKey.ColumnType.INTEGER),
                GroupKey.of("name", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("score", GroupKey.ColumnType.DOUBLE)
            ),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(4, ffi.fieldIndices().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(i, ffi.fieldIndices()[i]);
        }
    }

    // ── 7. Schema hash determinism ───────────────────────────────────────

    /**
     * The same definition must produce the same hash across multiple calls.
     */
    public void testSchemaHashDeterministic() {
        MVCompiledDefinition def1 = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("x", "sum_x"))
        );
        MVCompiledDefinition def2 = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("x", "sum_x"))
        );
        assertEquals(def1.hash(), def2.hash());
    }

    /**
     * The clickbench_100m compiled definition must produce the same hash
     * on every invocation — critical for schema drift detection.
     */
    public void testClickbench100mHashStable() {
        String hash1 = MVCompiledDefinition.clickbench100m().hash();
        String hash2 = MVCompiledDefinition.clickbench100m().hash();
        assertEquals(hash1, hash2);
    }

    /**
     * The hash must be a 64-character lowercase hex string (SHA-256).
     */
    public void testHashFormatIsSha256Hex() {
        String hash = singleKeyDefinition().hash();
        assertNotNull(hash);
        assertEquals("SHA-256 hex is 64 chars", 64, hash.length());
        assertTrue("hash must be lowercase hex", hash.matches("[0-9a-f]{64}"));
    }

    // ── 8. Definition hash mismatch detection ────────────────────────────

    /**
     * Different definitions must produce different hashes.
     */
    public void testDifferentDefinitionsProduceDifferentHashes() {
        MVCompiledDefinition defA = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition defB = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("x", "sum_x"))
        );
        assertNotEquals("Different aggregate lists must produce different hashes", defA.hash(), defB.hash());
    }

    /**
     * Changing a group key type must change the hash.
     */
    public void testGroupKeyTypeChangeAffectsHash() {
        MVCompiledDefinition defLong = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition defKeyword = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertNotEquals("Different key type must produce different hash", defLong.hash(), defKeyword.hash());
    }

    /**
     * Changing the group key name must change the hash.
     */
    public void testGroupKeyNameChangeAffectsHash() {
        MVCompiledDefinition defA = MVCompiledDefinition.of(
            List.of(GroupKey.of("alpha", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition defB = MVCompiledDefinition.of(
            List.of(GroupKey.of("beta", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertNotEquals("Different key name must produce different hash", defA.hash(), defB.hash());
    }

    /**
     * Adding a group key to the same aggregates must change the hash.
     */
    public void testAddingGroupKeyChangesHash() {
        MVCompiledDefinition defOne = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition defTwo = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG), GroupKey.of("k1", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertNotEquals("Adding group key must change hash", defOne.hash(), defTwo.hash());
    }

    /**
     * Expression-backed group key vs plain group key with same name must
     * produce different hashes (the expression contributes to canonical form).
     */
    public void testExpressionKeyDiffersFromPlainKey() {
        MVCompiledDefinition defPlain = MVCompiledDefinition.of(
            List.of(GroupKey.of("event_bucket", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition defExpr = MVCompiledDefinition.of(
            List.of(GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG, "CAST(\"EventTime\" AS BIGINT) / 300000", "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        assertNotEquals("Expression key must produce different hash from plain key", defPlain.hash(), defExpr.hash());
    }

    // ── 9. Ordering contract preserves full group tuple ───────────────────

    /**
     * Every group key — not just the first — must appear in the FFI
     * serialization with the correct state field index.
     */
    public void testOrderingPreservesAllGroupKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("a", GroupKey.ColumnType.LONG),
                GroupKey.of("b", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("c", GroupKey.ColumnType.INTEGER),
                GroupKey.of("d", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        // Every key must be present
        assertEquals(4, ffi.fieldIndices().length);
        for (int i = 0; i < 4; i++) {
            assertEquals(
                "Key " + i + " stateFieldIndex must match FFI fieldIndex",
                ordering.keys().get(i).stateFieldIndex(),
                ffi.fieldIndices()[i]
            );
            assertEquals(
                "Key " + i + " direction must match FFI directionToken",
                ordering.keys().get(i).direction().wireToken(),
                ffi.directionTokens()[i]
            );
            assertEquals(
                "Key " + i + " nullPlacement must match FFI nullPlacementToken",
                ordering.keys().get(i).nullPlacement().wireToken(),
                ffi.nullPlacementTokens()[i]
            );
        }
    }

    /**
     * The stateFieldIndices() list from the ordering must match the FFI
     * field indices exactly.
     */
    public void testStateFieldIndicesMatchFFI() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("x", GroupKey.ColumnType.LONG),
                GroupKey.of("y", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("z", GroupKey.ColumnType.INTEGER)
            ),
            List.of(AggregateSpec.sum("m", "sum_m"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        List<Integer> indices = ordering.stateFieldIndices();
        MVBuildRuntime.MVOrderingFFI ffi = MVBuildRuntime.MVOrderingFFI.from(ordering);

        assertEquals(indices.size(), ffi.fieldIndices().length);
        for (int i = 0; i < indices.size(); i++) {
            assertEquals(indices.get(i).intValue(), ffi.fieldIndices()[i]);
        }
    }

    /**
     * The column names from the ordering must match the group key names in order.
     */
    public void testOrderingColumnNamesMatchGroupKeyNames() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("EventTime", GroupKey.ColumnType.LONG),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );
        MVGroupByOrdering ordering = def.groupByOrdering();
        List<String> names = ordering.columnNames();

        assertEquals(3, names.size());
        assertEquals("EventTime", names.get(0));
        assertEquals("URL", names.get(1));
        assertEquals("UserID", names.get(2));
    }

    // ── 10. buildStreamingArtifact breaker accounting ─────────────────────

    /**
     * Breaker reservation must be released even when the build fails
     * (trip scenario). After the IOException, residual reservation must be 0.
     */
    public void testBreakerReleasedOnBuildStreamingArtifactFailure() throws Exception {
        long memEstimate = 32 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_release", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );

        assertEquals("Breaker must have zero reservation after failed build", 0L, breaker.getReserved());
        runtime.close();
    }

    /**
     * Multiple consecutive breaker trips on buildStreamingArtifact must
     * each throw independently and leave no residual reservation.
     */
    public void testBreakerRepeatableTripOnBuildStreamingArtifact() throws Exception {
        long memEstimate = 32 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_repeat", 1L);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        for (int i = 0; i < 3; i++) {
            expectThrows(
                IOException.class,
                () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
            );
        }
        assertEquals("Should have tripped 3 times", 3L, breaker.getTripCount());
        assertEquals("No residual reservation after repeated trips", 0L, breaker.getReserved());

        runtime.close();
    }

    /**
     * After close, breaker must have zero reservation — no leak from
     * buildStreamingArtifact lifecycle.
     */
    public void testBreakerZeroAfterCloseFollowingStreamingBuild() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_close_leak", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);
        runtime.close();

        assertEquals("No breaker leak after close", 0L, breaker.getReserved());
    }

    /**
     * With a null breaker, buildStreamingArtifact must still reject after
     * close without NPE.
     */
    public void testNullBreakerBuildStreamingArtifactRejectsAfterClose() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, 0, null);
        runtime.close();
        expectThrows(
            IllegalStateException.class,
            () -> runtime.buildStreamingArtifact("/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering())
        );
    }

    // ── 11. Extended ArtifactResult fields ────────────────────────────────

    /**
     * ArtifactResult with all new fields from native MvBuildResult.
     */
    public void testExtendedArtifactResultConstruction() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            42L,     // rowCount
            0xAABBL, // schemaHash
            0xCCDDL, // definitionHash
            0xEEFFL, // orderingHash
            1024L,   // spillBytes
            3,       // spillFileCount
            7,       // outputBatchCount
            65536L,  // peakRssBytes
            12345L,  // buildDurationUs
            MvBuildResultLayout.STATUS_OK // statusCode
        );
        assertEquals(42L, result.rowCount());
        assertEquals(0xAABBL, result.schemaHash());
        assertEquals(0xCCDDL, result.definitionHash());
        assertEquals(0xEEFFL, result.orderingHash());
        assertEquals(1024L, result.spillBytes());
        assertEquals(3, result.spillFileCount());
        assertEquals(7, result.outputBatchCount());
        assertEquals(65536L, result.peakRssBytes());
        assertEquals(12345L, result.buildDurationUs());
        assertEquals(MvBuildResultLayout.STATUS_OK, result.statusCode());
        assertTrue(result.isOk());
        assertFalse(result.isCancelled());
    }

    /**
     * ArtifactResult with cancelled status: rowCount may be 0.
     */
    public void testArtifactResultCancelledStatus() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            0L, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, MvBuildResultLayout.STATUS_CANCELLED
        );
        assertTrue(result.isCancelled());
        assertFalse(result.isOk());
        assertEquals(0L, result.rowCount());
    }

    /**
     * ArtifactResult with OK status must reject non-positive rowCount.
     */
    public void testArtifactResultOkStatusRejectsZeroRows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new MVBuildRuntime.ArtifactResult(
                0L, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, MvBuildResultLayout.STATUS_OK
            )
        );
    }

    /**
     * ArtifactResult with OK status must reject negative rowCount.
     */
    public void testArtifactResultOkStatusRejectsNegativeRows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new MVBuildRuntime.ArtifactResult(
                -1L, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, MvBuildResultLayout.STATUS_OK
            )
        );
    }

    /**
     * Backwards-compatible ArtifactResult constructor still works.
     */
    public void testBackwardsCompatibleArtifactResultConstructor() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(100L, "abc123", "def456");
        assertEquals(100L, result.rowCount());
        assertTrue(result.isOk());
    }

    // ── 12. Spill metrics on extended ArtifactResult ─────────────────────

    /**
     * ArtifactResult with non-zero spill data should be accessible.
     */
    public void testArtifactResultSpillMetrics() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            10L, 0L, 0L, 0L, 4096L, 2, 5, 0L, 0L, MvBuildResultLayout.STATUS_OK
        );
        assertEquals(4096L, result.spillBytes());
        assertEquals(2, result.spillFileCount());
        assertEquals(5, result.outputBatchCount());
    }

    /**
     * ArtifactResult with zero spill (no disk spill occurred).
     */
    public void testArtifactResultNoSpill() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            10L, 0L, 0L, 0L, 0L, 0, 3, 0L, 0L, MvBuildResultLayout.STATUS_OK
        );
        assertEquals(0L, result.spillBytes());
        assertEquals(0, result.spillFileCount());
    }

    // ── 13. RSS recording ────────────────────────────────────────────────

    /**
     * ArtifactResult with peak RSS should be accessible.
     */
    public void testArtifactResultPeakRss() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            10L, 0L, 0L, 0L, 0L, 0, 1, 256L * 1024 * 1024, 0L, MvBuildResultLayout.STATUS_OK
        );
        assertEquals(256L * 1024 * 1024, result.peakRssBytes());
    }

    // ── 14. Build duration ───────────────────────────────────────────────

    /**
     * ArtifactResult build duration in microseconds.
     */
    public void testArtifactResultBuildDuration() {
        MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
            10L, 0L, 0L, 0L, 0L, 0, 1, 0L, 5_000_000L, MvBuildResultLayout.STATUS_OK
        );
        assertEquals(5_000_000L, result.buildDurationUs());
    }

    // ── 15. MVBuildMetrics recordBuildDuration and recordOutputBatches ───

    public void testMetricsRecordBuildDuration() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordBuildDuration(1000L);
        metrics.recordBuildDuration(2000L);
        assertEquals(3000L, metrics.getTotalBuildDurationUs());
    }

    public void testMetricsRecordOutputBatches() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordOutputBatches(5);
        metrics.recordOutputBatches(3);
        assertEquals(8L, metrics.getTotalOutputBatches());
    }

    public void testMetricsSnapshotIncludesNewFields() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordBuildDuration(100L);
        metrics.recordOutputBatches(2);
        java.util.Map<String, Long> snapshot = metrics.snapshot();
        assertEquals(Long.valueOf(100L), snapshot.get("total_build_duration_us"));
        assertEquals(Long.valueOf(2L), snapshot.get("total_output_batches"));
    }

    public void testMetricsResetClearsNewFields() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordBuildDuration(100L);
        metrics.recordOutputBatches(2);
        metrics.reset();
        assertEquals(0L, metrics.getTotalBuildDurationUs());
        assertEquals(0L, metrics.getTotalOutputBatches());
    }
}
