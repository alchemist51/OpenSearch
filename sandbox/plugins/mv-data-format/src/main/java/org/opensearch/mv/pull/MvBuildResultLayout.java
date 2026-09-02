/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Panama FFM layout for decoding {@code MvBuildResult} from the native
 * {@code df_mv_build_streaming_result} FFI function.
 *
 * <p>The layout mirrors the Rust {@code #[repr(C)]} struct exactly, using
 * compile-time byte offsets so there is zero per-call overhead beyond a
 * single {@code MemorySegment.get()} per field.</p>
 *
 * <p>Usage:</p>
 * <pre>{@code
 * try (var call = new NativeCall()) {
 *     MemorySegment buf = call.buf(MvBuildResultLayout.STRUCT_SIZE);
 *     call.invoke(MV_BUILD_STREAMING_RESULT, runtimePtr, ..., buf);
 *     MvBuildResultLayout.validate(buf); // throws on version/size mismatch
 *     long rows = MvBuildResultLayout.rowCount(buf);
 *     int status = MvBuildResultLayout.statusCode(buf);
 *     // ...
 * }
 * }</pre>
 */
public final class MvBuildResultLayout {

    private MvBuildResultLayout() {}

    // ── Layout matches Rust #[repr(C)] MvBuildResult ─────────────────
    //
    // Offset  Size  Field
    // ------  ----  -----
    //  0       4    abi_version    (u32 → JAVA_INT)
    //  4       4    struct_size    (u32 → JAVA_INT)
    //  8       4    status_code    (i32 → JAVA_INT)
    // 12       4    _pad0          (u32, padding)
    // 16       8    row_count      (u64 → JAVA_LONG)
    // 24       8    schema_hash    (u64 → JAVA_LONG)
    // 32       8    definition_hash(u64 → JAVA_LONG)
    // 40       8    ordering_hash  (u64 → JAVA_LONG)
    // 48       8    spill_bytes    (u64 → JAVA_LONG)
    // 56       4    spill_file_count (u32 → JAVA_INT)
    // 60       4    output_batch_count (u32 → JAVA_INT)
    // 64       8    peak_rss_bytes (u64 → JAVA_LONG)
    // 72       8    build_duration_us (u64 → JAVA_LONG)
    // Total: 80 bytes (Java reads this prefix; Rust may write more)

    /** Java-side struct size: the number of bytes we read from the buffer. */
    public static final int STRUCT_SIZE = 80;

    /**
     * Minimum allocation size for the native out-buffer. Must be at least
     * {@code size_of::<MvBuildResult>()} on the Rust side (currently 88).
     * We allocate this many bytes; we read only {@link #STRUCT_SIZE} prefix.
     */
    public static final int NATIVE_ALLOC_SIZE = 88;

    /** Expected ABI version. Must match Rust {@code MvBuildResult::ABI_VERSION}. */
    public static final int EXPECTED_ABI_VERSION = 1;

    // ── Status code constants (must match Rust) ──────────────────────

    public static final int STATUS_OK = 0;
    public static final int STATUS_CANCELLED = 1;
    public static final int STATUS_SPILL_EXCEEDED = 2;
    public static final int STATUS_MEMORY_EXHAUSTED = 3;
    public static final int STATUS_INTERNAL_ERROR = -1;

    // ── Byte offsets (compile-time constants) ────────────────────────

    private static final long OFF_ABI_VERSION = 0;
    private static final long OFF_STRUCT_SIZE = 4;
    private static final long OFF_STATUS_CODE = 8;
    private static final long OFF_ROW_COUNT = 16;
    private static final long OFF_SCHEMA_HASH = 24;
    private static final long OFF_DEFINITION_HASH = 32;
    private static final long OFF_ORDERING_HASH = 40;
    private static final long OFF_SPILL_BYTES = 48;
    private static final long OFF_SPILL_FILE_COUNT = 56;
    private static final long OFF_OUTPUT_BATCH_COUNT = 60;
    private static final long OFF_PEAK_RSS_BYTES = 64;
    private static final long OFF_BUILD_DURATION_US = 72;

    // ── Validation ───────────────────────────────────────────────────

    /**
     * Validate the ABI version and struct size. Throws {@link IllegalStateException}
     * if the native struct is incompatible. Must be called before reading
     * any field.
     */
    public static void validate(MemorySegment buf) {
        int version = buf.get(ValueLayout.JAVA_INT, OFF_ABI_VERSION);
        if (version != EXPECTED_ABI_VERSION) {
            throw new IllegalStateException(
                "MvBuildResult ABI version mismatch: expected "
                    + EXPECTED_ABI_VERSION
                    + " but native returned "
                    + version
                    + ". The native library is incompatible with this Java build."
            );
        }
        int nativeSize = buf.get(ValueLayout.JAVA_INT, OFF_STRUCT_SIZE);
        if (nativeSize < STRUCT_SIZE) {
            throw new IllegalStateException(
                "MvBuildResult struct_size too small: native="
                    + nativeSize
                    + " java="
                    + STRUCT_SIZE
                    + ". The native library is older than this Java build."
            );
        }
        // nativeSize > STRUCT_SIZE is OK — Rust appended new fields that
        // this Java version does not yet know about. We read only our prefix.
    }

    // ── Field accessors ──────────────────────────────────────────────

    public static int statusCode(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_INT, OFF_STATUS_CODE);
    }

    public static boolean isOk(MemorySegment buf) {
        return statusCode(buf) == STATUS_OK;
    }

    public static long rowCount(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_ROW_COUNT);
    }

    public static long schemaHash(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_SCHEMA_HASH);
    }

    public static long definitionHash(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_DEFINITION_HASH);
    }

    public static long orderingHash(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_ORDERING_HASH);
    }

    public static long spillBytes(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_SPILL_BYTES);
    }

    public static int spillFileCount(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_INT, OFF_SPILL_FILE_COUNT);
    }

    public static int outputBatchCount(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_INT, OFF_OUTPUT_BATCH_COUNT);
    }

    public static long peakRssBytes(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_PEAK_RSS_BYTES);
    }

    public static long buildDurationUs(MemorySegment buf) {
        return buf.get(ValueLayout.JAVA_LONG, OFF_BUILD_DURATION_US);
    }
}
