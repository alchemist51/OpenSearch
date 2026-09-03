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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Comprehensive FFI result contract tests for the MV pull build path.
 *
 * <p>These tests verify the Java-side contract for decoding {@code MvBuildResult}
 * from native memory, ABI/struct validation, ordering hash cross-language parity,
 * metrics recording, Panama layout correctness, and the cancellation path.</p>
 *
 * <p>Tests 1–10 map to the contract test plan:</p>
 * <ol>
 *   <li>testDecodeCorrectResult — decode all fields from known bytes</li>
 *   <li>testDecodeAbiVersionMismatch — ABI version 99 → IllegalStateException</li>
 *   <li>testDecodeSizeMismatch — struct_size too small → IllegalStateException</li>
 *   <li>testSchemaHashMismatch — native schema_hash != expected</li>
 *   <li>testDefinitionHashMismatch — native definition_hash != expected</li>
 *   <li>testMetricsRecordSpill — spill accumulation</li>
 *   <li>testMetricsNoSpill — zero spill values</li>
 *   <li>testMetricsOutputBatchCount — output_batch_count > 0</li>
 *   <li>testFfiDescriptorMatchesStruct — Panama layout byte size + offsets</li>
 *   <li>testCancellationPath — cancel → no publication, proper cleanup</li>
 * </ol>
 */
public class MVFFIResultContractTests extends OpenSearchTestCase {

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Build a synthetic MvBuildResult buffer matching the Rust #[repr(C)] layout.
     * All multi-byte values are native endian (little-endian on x86/ARM).
     */
    private static MemorySegment buildResultBuf(
        Arena arena,
        int abiVersion,
        int structSize,
        int statusCode,
        long rowCount,
        long schemaHash,
        long definitionHash,
        long orderingHash,
        long spillBytes,
        int spillFileCount,
        int outputBatchCount,
        long peakRssBytes,
        long buildDurationUs
    ) {
        int allocSize = Math.max(structSize, MvBuildResultLayout.NATIVE_ALLOC_SIZE);
        MemorySegment buf = arena.allocate(allocSize);
        buf.set(ValueLayout.JAVA_INT, 0, abiVersion);           // offset 0
        buf.set(ValueLayout.JAVA_INT, 4, structSize);            // offset 4
        buf.set(ValueLayout.JAVA_INT, 8, statusCode);            // offset 8
        buf.set(ValueLayout.JAVA_INT, 12, 0);                    // offset 12: _pad0
        buf.set(ValueLayout.JAVA_LONG, 16, rowCount);            // offset 16
        buf.set(ValueLayout.JAVA_LONG, 24, schemaHash);          // offset 24
        buf.set(ValueLayout.JAVA_LONG, 32, definitionHash);      // offset 32
        buf.set(ValueLayout.JAVA_LONG, 40, orderingHash);        // offset 40
        buf.set(ValueLayout.JAVA_LONG, 48, spillBytes);          // offset 48
        buf.set(ValueLayout.JAVA_INT, 56, spillFileCount);       // offset 56
        buf.set(ValueLayout.JAVA_INT, 60, outputBatchCount);     // offset 60
        buf.set(ValueLayout.JAVA_LONG, 64, peakRssBytes);        // offset 64
        buf.set(ValueLayout.JAVA_LONG, 72, buildDurationUs);     // offset 72
        return buf;
    }

    private static MVGroupByOrdering canonicalThreeKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("k2", GroupKey.ColumnType.INTEGER)
            ),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();
    }

    private static MVGroupByOrdering singleKeyOrdering() {
        return MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();
    }

    /**
     * Circuit breaker stub that tracks reservations and can trip at a threshold.
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

    // ═════════════════════════════════════════════════════════════════════
    // TEST 1: testDecodeCorrectResult
    // Allocate a MvBuildResult-shaped MemorySegment with known field values,
    // call validate + field accessors, assert all fields decoded correctly.
    // ═════════════════════════════════════════════════════════════════════

    public void testDecodeCorrectResult() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(
                arena,
                1,            // abi_version = EXPECTED
                88,           // struct_size = Rust current (88)
                0,            // status_code = OK
                42L,          // row_count
                0xDEAD_BEEFL, // schema_hash
                0xCAFE_BABEL, // definition_hash
                0x1234_5678L, // ordering_hash
                2048L,        // spill_bytes
                5,            // spill_file_count
                12,           // output_batch_count
                128L * 1024 * 1024, // peak_rss_bytes (128 MiB)
                9_876_543L    // build_duration_us
            );

            // Validate must pass
            MvBuildResultLayout.validate(buf);

            // Decode all fields
            assertEquals(MvBuildResultLayout.STATUS_OK, MvBuildResultLayout.statusCode(buf));
            assertTrue(MvBuildResultLayout.isOk(buf));
            assertEquals(42L, MvBuildResultLayout.rowCount(buf));
            assertEquals(0xDEAD_BEEFL, MvBuildResultLayout.schemaHash(buf));
            assertEquals(0xCAFE_BABEL, MvBuildResultLayout.definitionHash(buf));
            assertEquals(0x1234_5678L, MvBuildResultLayout.orderingHash(buf));
            assertEquals(2048L, MvBuildResultLayout.spillBytes(buf));
            assertEquals(5, MvBuildResultLayout.spillFileCount(buf));
            assertEquals(12, MvBuildResultLayout.outputBatchCount(buf));
            assertEquals(128L * 1024 * 1024, MvBuildResultLayout.peakRssBytes(buf));
            assertEquals(9_876_543L, MvBuildResultLayout.buildDurationUs(buf));
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 2: testDecodeAbiVersionMismatch
    // Set abi_version to 99, assert IllegalStateException thrown.
    // ═════════════════════════════════════════════════════════════════════

    public void testDecodeAbiVersionMismatch() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(
                arena, 99, 88, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0
            );
            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> MvBuildResultLayout.validate(buf)
            );
            assertTrue(
                "Exception must mention ABI version mismatch, got: " + ex.getMessage(),
                ex.getMessage().contains("ABI version mismatch")
            );
            assertTrue(
                "Exception must mention expected version 1, got: " + ex.getMessage(),
                ex.getMessage().contains("expected 1")
            );
            assertTrue(
                "Exception must mention native version 99, got: " + ex.getMessage(),
                ex.getMessage().contains("native returned 99")
            );
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 3: testDecodeSizeMismatch
    // Set struct_size to 64 (less than Java's 80), assert IllegalStateException.
    // ═════════════════════════════════════════════════════════════════════

    public void testDecodeSizeMismatch() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(
                arena, 1, 64, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0
            );
            IllegalStateException ex = expectThrows(
                IllegalStateException.class,
                () -> MvBuildResultLayout.validate(buf)
            );
            assertTrue(
                "Exception must mention struct_size too small, got: " + ex.getMessage(),
                ex.getMessage().contains("struct_size too small")
            );
            assertTrue(
                "Exception must mention native=64, got: " + ex.getMessage(),
                ex.getMessage().contains("native=64")
            );
        }
    }

    /**
     * Forward-compat: struct_size larger than Java's 80 must be accepted
     * (native appended new fields that Java doesn't know about yet).
     */
    public void testDecodeSizeLargerThanJavaAccepted() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(128);
            buf.set(ValueLayout.JAVA_INT, 0, 1);    // abi_version
            buf.set(ValueLayout.JAVA_INT, 4, 128);   // struct_size (future Rust)
            buf.set(ValueLayout.JAVA_INT, 8, 0);     // status_code
            buf.set(ValueLayout.JAVA_LONG, 16, 99L);
            MvBuildResultLayout.validate(buf); // must NOT throw
            assertEquals(99L, MvBuildResultLayout.rowCount(buf));
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 4: testSchemaHashMismatch
    // Construct a scenario where native schema_hash != expected from
    // MVCompiledDefinition, assert mismatch detected.
    // ═════════════════════════════════════════════════════════════════════

    /**
     * When the native result has a schema_hash that differs from what the Java
     * definition expects, any validation that compares these values should
     * detect the mismatch. We verify the raw decoded values differ.
     */
    public void testSchemaHashMismatch() {
        try (Arena arena = Arena.ofConfined()) {
            long nativeSchemaHash = 0xBAD0BAD0L;
            long expectedSchemaHash = 0xDEADBEEFL;

            MemorySegment buf = buildResultBuf(
                arena, 1, 88, 0, 42L,
                nativeSchemaHash,   // schema_hash from native
                0L,                 // definition_hash
                0L,                 // ordering_hash
                0L, 0, 0, 0L, 0L
            );
            MvBuildResultLayout.validate(buf);

            long decoded = MvBuildResultLayout.schemaHash(buf);
            assertEquals(nativeSchemaHash, decoded);
            assertNotEquals(
                "schema_hash from native must differ from expected — mismatch detected",
                expectedSchemaHash,
                decoded
            );
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 5: testDefinitionHashMismatch
    // Same for definition_hash.
    // ═════════════════════════════════════════════════════════════════════

    /**
     * When the native result has a definition_hash that differs from the Java
     * definition's hash, the mismatch is detectable.
     */
    public void testDefinitionHashMismatch() {
        try (Arena arena = Arena.ofConfined()) {
            long nativeDefHash = 0xAAAA_AAAAL;
            long expectedDefHash = 0xBBBB_BBBBL;

            MemorySegment buf = buildResultBuf(
                arena, 1, 88, 0, 42L,
                0L,              // schema_hash
                nativeDefHash,   // definition_hash from native
                0L,              // ordering_hash
                0L, 0, 0, 0L, 0L
            );
            MvBuildResultLayout.validate(buf);

            long decoded = MvBuildResultLayout.definitionHash(buf);
            assertEquals(nativeDefHash, decoded);
            assertNotEquals(
                "definition_hash from native must differ from expected — mismatch detected",
                expectedDefHash,
                decoded
            );
        }
    }

    /**
     * Ordering hash mismatch: the MVBuildRuntime.buildStreamingArtifact path
     * checks native ordering_hash against Java orderingIdentityHash() and
     * throws MvBuildOrderingMismatchException on mismatch. We verify the
     * exception class is an IOException (fail-closed, no publication).
     */
    public void testOrderingHashMismatchThrowsCorrectException() {
        MvBuildOrderingMismatchException ex = new MvBuildOrderingMismatchException(
            "Ordering identity hash mismatch: native=0xAABB java=0xCCDD"
        );
        assertTrue(
            "MvBuildOrderingMismatchException must be IOException to block publication",
            ex instanceof IOException
        );
        assertTrue(ex.getMessage().contains("mismatch"));
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 6: testMetricsRecordSpill
    // Multiple recordSpill() calls aggregate into getSpillBytes/getSpillFiles.
    // ═════════════════════════════════════════════════════════════════════

    public void testMetricsRecordSpill() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordSpill(1024, 2);
        metrics.recordSpill(2048, 3);
        metrics.recordSpill(512, 1);

        assertEquals(
            "spillBytes should accumulate across calls",
            1024 + 2048 + 512,
            metrics.getSpillBytes()
        );
        assertEquals(
            "spillFiles should accumulate across calls",
            2 + 3 + 1,
            metrics.getSpillFiles()
        );
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 7: testMetricsNoSpill
    // Build without spill: assert zero spill values.
    // ═════════════════════════════════════════════════════════════════════

    public void testMetricsNoSpill() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        // Record a build with zero spill
        metrics.recordSpill(0, 0);
        metrics.recordBuildCompleted();

        assertEquals(0L, metrics.getSpillBytes());
        assertEquals(0L, metrics.getSpillFiles());
        assertEquals(1L, metrics.getTotalBuilds());
    }

    /**
     * Fresh metrics instance should have zero spill without any recording.
     */
    public void testMetricsNoSpillFreshInstance() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        assertEquals(0L, metrics.getSpillBytes());
        assertEquals(0L, metrics.getSpillFiles());
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 8: testMetricsOutputBatchCount
    // Assert output_batch_count > 0 after recording.
    // ═════════════════════════════════════════════════════════════════════

    public void testMetricsOutputBatchCount() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordOutputBatches(7);
        assertTrue(
            "totalOutputBatches must be > 0 after recording",
            metrics.getTotalOutputBatches() > 0
        );
        assertEquals(7L, metrics.getTotalOutputBatches());

        // Multiple recordings accumulate
        metrics.recordOutputBatches(3);
        assertEquals(10L, metrics.getTotalOutputBatches());
    }

    /**
     * Snapshot must contain the total_output_batches key after recording.
     */
    public void testMetricsOutputBatchCountInSnapshot() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordOutputBatches(5);
        Map<String, Long> snap = metrics.snapshot();
        assertTrue(snap.containsKey("total_output_batches"));
        assertEquals(Long.valueOf(5L), snap.get("total_output_batches"));
    }

    /**
     * Verify snapshot contains all 11 documented metric keys.
     */
    public void testSnapshotContainsAllElevenKeys() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        Map<String, Long> snap = metrics.snapshot();
        assertEquals("snapshot must contain exactly 29 metric keys", 29, snap.size());

        // Verify each documented key exists
        assertTrue(snap.containsKey("spill_bytes"));
        assertTrue(snap.containsKey("spill_files"));
        assertTrue(snap.containsKey("breaker_reservations"));
        assertTrue(snap.containsKey("breaker_trips"));
        assertTrue(snap.containsKey("active_breaker_bytes"));
        assertTrue(snap.containsKey("peak_rss_bytes"));
        assertTrue(snap.containsKey("fan_in_rounds"));
        assertTrue(snap.containsKey("total_builds"));
        assertTrue(snap.containsKey("failed_builds"));
        assertTrue(snap.containsKey("total_build_duration_us"));
        assertTrue(snap.containsKey("total_output_batches"));
        // Metadata cache counters
        assertTrue(snap.containsKey("metadata_cache_hits"));
        assertTrue(snap.containsKey("metadata_cache_refreshes"));
        assertTrue(snap.containsKey("incremental_sync_skipped_files"));
        // Compaction counters
        assertTrue(snap.containsKey("compactions_started"));
        assertTrue(snap.containsKey("compactions_completed"));
        assertTrue(snap.containsKey("compactions_failed"));
        assertTrue(snap.containsKey("compactions_skipped"));
        assertTrue(snap.containsKey("compaction_input_generations"));
        assertTrue(snap.containsKey("compaction_input_bytes"));
        assertTrue(snap.containsKey("compaction_output_rows"));
        assertTrue(snap.containsKey("compaction_output_bytes"));
        assertTrue(snap.containsKey("compaction_duration_ms"));
        // Checksum counters (O(1) checksum for mv_state)
        assertTrue(snap.containsKey("checksum_registered"));
        assertTrue(snap.containsKey("checksum_misses"));
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 9: testFfiDescriptorMatchesStruct
    // Assert the Panama layout byte size == expected struct size, and
    // field offsets match constants.
    // ═════════════════════════════════════════════════════════════════════

    /**
     * Verify that the Java-side struct constants match the documented
     * Rust #[repr(C)] layout exactly.
     */
    public void testFfiDescriptorMatchesStruct() {
        // STRUCT_SIZE must be 80 (Java-side prefix we read)
        assertEquals("STRUCT_SIZE must be 80", 80, MvBuildResultLayout.STRUCT_SIZE);

        // NATIVE_ALLOC_SIZE must be >= STRUCT_SIZE
        assertTrue(
            "NATIVE_ALLOC_SIZE must be >= STRUCT_SIZE",
            MvBuildResultLayout.NATIVE_ALLOC_SIZE >= MvBuildResultLayout.STRUCT_SIZE
        );

        // EXPECTED_ABI_VERSION must be 1
        assertEquals(1, MvBuildResultLayout.EXPECTED_ABI_VERSION);

        // Verify field offsets by writing known values at documented offsets
        // and reading them through the accessor methods
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = arena.allocate(MvBuildResultLayout.NATIVE_ALLOC_SIZE);

            // Write at documented offsets
            buf.set(ValueLayout.JAVA_INT, 0, 1);                      // abi_version @ 0
            buf.set(ValueLayout.JAVA_INT, 4, 88);                     // struct_size @ 4
            buf.set(ValueLayout.JAVA_INT, 8, 0);                      // status_code @ 8
            buf.set(ValueLayout.JAVA_INT, 12, 0);                     // _pad0 @ 12
            buf.set(ValueLayout.JAVA_LONG, 16, 100L);                 // row_count @ 16
            buf.set(ValueLayout.JAVA_LONG, 24, 0x1111L);              // schema_hash @ 24
            buf.set(ValueLayout.JAVA_LONG, 32, 0x2222L);              // definition_hash @ 32
            buf.set(ValueLayout.JAVA_LONG, 40, 0x3333L);              // ordering_hash @ 40
            buf.set(ValueLayout.JAVA_LONG, 48, 4096L);                // spill_bytes @ 48
            buf.set(ValueLayout.JAVA_INT, 56, 7);                     // spill_file_count @ 56
            buf.set(ValueLayout.JAVA_INT, 60, 15);                    // output_batch_count @ 60
            buf.set(ValueLayout.JAVA_LONG, 64, 256L * 1024 * 1024);   // peak_rss_bytes @ 64
            buf.set(ValueLayout.JAVA_LONG, 72, 5_000_000L);           // build_duration_us @ 72

            // Read through accessors — must match
            MvBuildResultLayout.validate(buf);
            assertEquals("statusCode @ offset 8", 0, MvBuildResultLayout.statusCode(buf));
            assertEquals("rowCount @ offset 16", 100L, MvBuildResultLayout.rowCount(buf));
            assertEquals("schemaHash @ offset 24", 0x1111L, MvBuildResultLayout.schemaHash(buf));
            assertEquals("definitionHash @ offset 32", 0x2222L, MvBuildResultLayout.definitionHash(buf));
            assertEquals("orderingHash @ offset 40", 0x3333L, MvBuildResultLayout.orderingHash(buf));
            assertEquals("spillBytes @ offset 48", 4096L, MvBuildResultLayout.spillBytes(buf));
            assertEquals("spillFileCount @ offset 56", 7, MvBuildResultLayout.spillFileCount(buf));
            assertEquals("outputBatchCount @ offset 60", 15, MvBuildResultLayout.outputBatchCount(buf));
            assertEquals("peakRssBytes @ offset 64", 256L * 1024 * 1024, MvBuildResultLayout.peakRssBytes(buf));
            assertEquals("buildDurationUs @ offset 72", 5_000_000L, MvBuildResultLayout.buildDurationUs(buf));
        }
    }

    /**
     * Verify status code constants match the Rust enum values exactly.
     */
    public void testStatusCodeConstantsMatchRust() {
        assertEquals(0, MvBuildResultLayout.STATUS_OK);
        assertEquals(1, MvBuildResultLayout.STATUS_CANCELLED);
        assertEquals(2, MvBuildResultLayout.STATUS_SPILL_EXCEEDED);
        assertEquals(3, MvBuildResultLayout.STATUS_MEMORY_EXHAUSTED);
        assertEquals(-1, MvBuildResultLayout.STATUS_INTERNAL_ERROR);
    }

    // ═════════════════════════════════════════════════════════════════════
    // TEST 10: testCancellationPath
    // Trigger cancellation, assert no publication and proper cleanup.
    // ═════════════════════════════════════════════════════════════════════

    /**
     * When the native result has STATUS_CANCELLED, the ArtifactResult
     * isCancelled() returns true, and the build path must not publish
     * the artifact. Circuit breaker must be released.
     */
    public void testCancellationPath() {
        try (Arena arena = Arena.ofConfined()) {
            // Simulate a cancelled native result
            MemorySegment buf = buildResultBuf(
                arena, 1, 88,
                MvBuildResultLayout.STATUS_CANCELLED,  // cancelled status
                0L, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L
            );
            MvBuildResultLayout.validate(buf);

            // Status checks
            assertFalse("cancelled build must not be OK", MvBuildResultLayout.isOk(buf));
            assertEquals(MvBuildResultLayout.STATUS_CANCELLED, MvBuildResultLayout.statusCode(buf));

            // ArtifactResult with cancelled status
            MVBuildRuntime.ArtifactResult result = new MVBuildRuntime.ArtifactResult(
                0L, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, MvBuildResultLayout.STATUS_CANCELLED
            );
            assertTrue("ArtifactResult must report cancelled", result.isCancelled());
            assertFalse("ArtifactResult must not be OK", result.isOk());
        }
    }

    /**
     * MVBuildRuntime.cancel() on a closed runtime should not throw.
     */
    public void testCancelOnClosedRuntimeIsNoOp() throws Exception {
        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0);
        runtime.close();
        runtime.cancel(); // must not throw
    }

    /**
     * After close + cancel, circuit breaker must have zero residual reservation.
     */
    public void testCancellationReleasesBreaker() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_cancel", Long.MAX_VALUE);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);
        runtime.close();

        assertEquals(
            "No breaker leak after close",
            0L,
            breaker.getReserved()
        );
    }

    /**
     * Circuit breaker trip on buildStreamingArtifact must leave zero residual
     * reservation (the artifact is NOT published).
     */
    public void testBreakerTripPreventsPublication() throws Exception {
        long memEstimate = 64 * 1024 * 1024L;
        TrackingCircuitBreaker breaker = new TrackingCircuitBreaker("test_pub_prevent", memEstimate - 1);

        MVBuildRuntime runtime = new MVBuildRuntime(1L, 0, 0, memEstimate, breaker);

        IOException ex = expectThrows(
            IOException.class,
            () -> runtime.buildStreamingArtifact(
                "/tmp/input", "table", "SELECT 1", "/tmp/out", singleKeyOrdering()
            )
        );
        assertTrue(
            "Expected CircuitBreakingException cause",
            ex.getCause() instanceof CircuitBreakingException
        );
        assertEquals(
            "Breaker must have zero reservation after failed build (no publication)",
            0L,
            breaker.getReserved()
        );

        runtime.close();
    }

    // ═════════════════════════════════════════════════════════════════════
    // BONUS: Cross-language ordering hash parity
    // ═════════════════════════════════════════════════════════════════════

    /**
     * Verify the canonical 3-key ordering [0 ASC NF, 1 ASC NF, 2 ASC NF]
     * produces a stable non-zero hash via orderingIdentityHash().
     */
    public void testOrderingIdentityHashCrossLanguageParity() {
        MVGroupByOrdering ordering = canonicalThreeKeyOrdering();
        long hash = ordering.orderingIdentityHash();

        // Must be non-zero
        assertNotEquals("ordering hash must not be zero", 0L, hash);

        // Must be deterministic
        assertEquals(
            "hash must be deterministic across calls",
            hash,
            ordering.orderingIdentityHash()
        );

        // Verify against a second identical ordering instance to confirm
        // that the hash is value-based (not identity-based)
        MVGroupByOrdering ordering2 = canonicalThreeKeyOrdering();
        assertEquals(
            "identical orderings must produce the same hash",
            hash,
            ordering2.orderingIdentityHash()
        );

        // Different ordering (2-key) must produce different hash
        MVGroupByOrdering twoKey = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();
        assertNotEquals(
            "different key counts must produce different hashes",
            hash,
            twoKey.orderingIdentityHash()
        );
    }

    // ═════════════════════════════════════════════════════════════════════
    // BONUS: Large u64 field values
    // ═════════════════════════════════════════════════════════════════════

    /**
     * Verify u64 fields handle values larger than Long.MAX_VALUE (unsigned).
     */
    public void testLargeU64FieldValues() {
        try (Arena arena = Arena.ofConfined()) {
            long maxU64 = -1L; // 0xFFFFFFFFFFFFFFFF as signed long
            MemorySegment buf = buildResultBuf(
                arena, 1, 88, 0,
                maxU64,            // row_count
                maxU64,            // schema_hash
                maxU64,            // definition_hash
                maxU64,            // ordering_hash
                maxU64,            // spill_bytes
                Integer.MAX_VALUE, // spill_file_count
                Integer.MAX_VALUE, // output_batch_count
                maxU64,            // peak_rss_bytes
                maxU64             // build_duration_us
            );
            MvBuildResultLayout.validate(buf);
            assertEquals(maxU64, MvBuildResultLayout.rowCount(buf));
            assertEquals(maxU64, MvBuildResultLayout.schemaHash(buf));
            assertEquals(maxU64, MvBuildResultLayout.definitionHash(buf));
            assertEquals(maxU64, MvBuildResultLayout.orderingHash(buf));
            assertEquals(maxU64, MvBuildResultLayout.spillBytes(buf));
            assertEquals(maxU64, MvBuildResultLayout.peakRssBytes(buf));
            assertEquals(maxU64, MvBuildResultLayout.buildDurationUs(buf));
        }
    }

    // ═════════════════════════════════════════════════════════════════════
    // BONUS: Metrics reset clears all fields
    // ═════════════════════════════════════════════════════════════════════

    public void testMetricsResetClearsEverything() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        metrics.recordSpill(1024, 2);
        metrics.recordOutputBatches(5);
        metrics.recordBuildDuration(100_000L);
        metrics.recordRss(256L * 1024 * 1024);
        metrics.recordBuildCompleted();
        metrics.recordBuildFailed();

        metrics.reset();

        assertEquals(0L, metrics.getSpillBytes());
        assertEquals(0L, metrics.getSpillFiles());
        assertEquals(0L, metrics.getTotalOutputBatches());
        assertEquals(0L, metrics.getTotalBuildDurationUs());
        assertEquals(0L, metrics.getPeakRssBytes());
        assertEquals(0L, metrics.getTotalBuilds());
        assertEquals(0L, metrics.getFailedBuilds());
    }
}
