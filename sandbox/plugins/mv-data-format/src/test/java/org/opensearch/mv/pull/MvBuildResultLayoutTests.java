/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Unit tests for {@link MvBuildResultLayout}: byte-level decode, ABI version
 * validation, forward-compatibility, and field accessor correctness.
 */
public class MvBuildResultLayoutTests extends OpenSearchTestCase {

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Build a synthetic MvBuildResult buffer matching the Rust layout.
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
        MemorySegment buf = arena.allocate(Math.max(structSize, MvBuildResultLayout.STRUCT_SIZE));
        buf.set(ValueLayout.JAVA_INT, 0, abiVersion);
        buf.set(ValueLayout.JAVA_INT, 4, structSize);
        buf.set(ValueLayout.JAVA_INT, 8, statusCode);
        buf.set(ValueLayout.JAVA_INT, 12, 0); // _pad0
        buf.set(ValueLayout.JAVA_LONG, 16, rowCount);
        buf.set(ValueLayout.JAVA_LONG, 24, schemaHash);
        buf.set(ValueLayout.JAVA_LONG, 32, definitionHash);
        buf.set(ValueLayout.JAVA_LONG, 40, orderingHash);
        buf.set(ValueLayout.JAVA_LONG, 48, spillBytes);
        buf.set(ValueLayout.JAVA_INT, 56, spillFileCount);
        buf.set(ValueLayout.JAVA_INT, 60, outputBatchCount);
        buf.set(ValueLayout.JAVA_LONG, 64, peakRssBytes);
        buf.set(ValueLayout.JAVA_LONG, 72, buildDurationUs);
        return buf;
    }

    private static MemorySegment buildOkResult(Arena arena) {
        return buildResultBuf(arena, 1, 88, 0, 42L, 0xAABBL, 0xCCDDL, 0xEEFFL, 1024L, 3, 7, 65536L, 12345L);
    }

    // ── 1. Basic decode ──────────────────────────────────────────────────

    public void testDecodeAllFieldsFromOkResult() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildOkResult(arena);
            MvBuildResultLayout.validate(buf);

            assertEquals(MvBuildResultLayout.STATUS_OK, MvBuildResultLayout.statusCode(buf));
            assertTrue(MvBuildResultLayout.isOk(buf));
            assertEquals(42L, MvBuildResultLayout.rowCount(buf));
            assertEquals(0xAABBL, MvBuildResultLayout.schemaHash(buf));
            assertEquals(0xCCDDL, MvBuildResultLayout.definitionHash(buf));
            assertEquals(0xEEFFL, MvBuildResultLayout.orderingHash(buf));
            assertEquals(1024L, MvBuildResultLayout.spillBytes(buf));
            assertEquals(3, MvBuildResultLayout.spillFileCount(buf));
            assertEquals(7, MvBuildResultLayout.outputBatchCount(buf));
            assertEquals(65536L, MvBuildResultLayout.peakRssBytes(buf));
            assertEquals(12345L, MvBuildResultLayout.buildDurationUs(buf));
        }
    }

    // ── 2. ABI version validation ────────────────────────────────────────

    public void testValidateRejectsWrongAbiVersion() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 2, 88, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> MvBuildResultLayout.validate(buf));
            assertTrue(ex.getMessage().contains("ABI version mismatch"));
            assertTrue(ex.getMessage().contains("expected 1"));
            assertTrue(ex.getMessage().contains("native returned 2"));
        }
    }

    public void testValidateRejectsZeroAbiVersion() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 0, 88, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
            expectThrows(IllegalStateException.class, () -> MvBuildResultLayout.validate(buf));
        }
    }

    // ── 3. Struct size validation ────────────────────────────────────────

    public void testValidateRejectsTooSmallStructSize() {
        try (Arena arena = Arena.ofConfined()) {
            // Native claims only 64 bytes — smaller than Java needs
            MemorySegment buf = buildResultBuf(arena, 1, 64, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
            IllegalStateException ex = expectThrows(IllegalStateException.class, () -> MvBuildResultLayout.validate(buf));
            assertTrue(ex.getMessage().contains("struct_size too small"));
            assertTrue(ex.getMessage().contains("native=64"));
        }
    }

    public void testValidateAcceptsExactStructSize() {
        try (Arena arena = Arena.ofConfined()) {
            // Native reports exactly 80 bytes — matches Java layout
            MemorySegment buf = buildResultBuf(arena, 1, 80, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf); // should not throw
        }
    }

    // ── 4. Forward compatibility ─────────────────────────────────────────

    /**
     * When native reports a larger struct_size (e.g., 88 or 96), validation
     * succeeds. Java reads only its known prefix and ignores trailing fields.
     */
    public void testValidateAcceptsLargerStructSize() {
        try (Arena arena = Arena.ofConfined()) {
            // Native has 88 bytes (current Rust size) — larger than Java's 80
            MemorySegment buf = arena.allocate(96);
            buf.set(ValueLayout.JAVA_INT, 0, 1);  // abi_version
            buf.set(ValueLayout.JAVA_INT, 4, 96);  // struct_size (future)
            buf.set(ValueLayout.JAVA_INT, 8, 0);   // status_code
            buf.set(ValueLayout.JAVA_LONG, 16, 100L); // row_count
            MvBuildResultLayout.validate(buf); // should not throw
            assertEquals(100L, MvBuildResultLayout.rowCount(buf));
        }
    }

    // ── 5. Status code accessors ─────────────────────────────────────────

    public void testCancelledStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 1, 88, MvBuildResultLayout.STATUS_CANCELLED, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf);
            assertFalse(MvBuildResultLayout.isOk(buf));
            assertEquals(MvBuildResultLayout.STATUS_CANCELLED, MvBuildResultLayout.statusCode(buf));
        }
    }

    public void testSpillExceededStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 1, 88, MvBuildResultLayout.STATUS_SPILL_EXCEEDED, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf);
            assertFalse(MvBuildResultLayout.isOk(buf));
            assertEquals(MvBuildResultLayout.STATUS_SPILL_EXCEEDED, MvBuildResultLayout.statusCode(buf));
        }
    }

    public void testMemoryExhaustedStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 1, 88, MvBuildResultLayout.STATUS_MEMORY_EXHAUSTED, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf);
            assertFalse(MvBuildResultLayout.isOk(buf));
            assertEquals(MvBuildResultLayout.STATUS_MEMORY_EXHAUSTED, MvBuildResultLayout.statusCode(buf));
        }
    }

    public void testInternalErrorStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 1, 88, MvBuildResultLayout.STATUS_INTERNAL_ERROR, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf);
            assertFalse(MvBuildResultLayout.isOk(buf));
            assertEquals(MvBuildResultLayout.STATUS_INTERNAL_ERROR, MvBuildResultLayout.statusCode(buf));
        }
    }

    // ── 6. Large u64 values ──────────────────────────────────────────────

    /**
     * Verify that u64 fields correctly handle values larger than Long.MAX_VALUE
     * (unsigned representation).
     */
    public void testLargeU64Values() {
        try (Arena arena = Arena.ofConfined()) {
            long maxU64 = -1L; // 0xFFFFFFFFFFFFFFFF as signed long
            MemorySegment buf = buildResultBuf(arena, 1, 88, 0, maxU64, maxU64, maxU64, maxU64, maxU64, Integer.MAX_VALUE, Integer.MAX_VALUE, maxU64, maxU64);
            MvBuildResultLayout.validate(buf);
            assertEquals(maxU64, MvBuildResultLayout.rowCount(buf));
            assertEquals(maxU64, MvBuildResultLayout.schemaHash(buf));
            assertEquals(maxU64, MvBuildResultLayout.spillBytes(buf));
            assertEquals(maxU64, MvBuildResultLayout.peakRssBytes(buf));
            assertEquals(maxU64, MvBuildResultLayout.buildDurationUs(buf));
        }
    }

    // ── 7. Zero-valued OK result ─────────────────────────────────────────

    /**
     * All data fields zero with status OK should decode cleanly.
     */
    public void testZeroFieldsOkStatus() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment buf = buildResultBuf(arena, 1, 80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            MvBuildResultLayout.validate(buf);
            assertTrue(MvBuildResultLayout.isOk(buf));
            assertEquals(0L, MvBuildResultLayout.rowCount(buf));
            assertEquals(0L, MvBuildResultLayout.spillBytes(buf));
            assertEquals(0, MvBuildResultLayout.spillFileCount(buf));
        }
    }

    // ── 8. Status code constants pinned ──────────────────────────────────

    public void testStatusCodeConstantsMatchRust() {
        assertEquals(0, MvBuildResultLayout.STATUS_OK);
        assertEquals(1, MvBuildResultLayout.STATUS_CANCELLED);
        assertEquals(2, MvBuildResultLayout.STATUS_SPILL_EXCEEDED);
        assertEquals(3, MvBuildResultLayout.STATUS_MEMORY_EXHAUSTED);
        assertEquals(-1, MvBuildResultLayout.STATUS_INTERNAL_ERROR);
    }

    // ── 9. Layout constants pinned ───────────────────────────────────────

    public void testStructSizeConstant() {
        assertEquals(80, MvBuildResultLayout.STRUCT_SIZE);
    }

    public void testExpectedAbiVersionConstant() {
        assertEquals(1, MvBuildResultLayout.EXPECTED_ABI_VERSION);
    }
}
