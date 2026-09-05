/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * POC(mv): minimal FFI binding for the MV state-file build. Binds
 * {@code df_mv_build_managed} from the shared native lib directly — no dependency
 * on the DataFusion plugin's Java classes (avoids cross-plugin classloader
 * coupling; both crates' symbols live in the one shared .so).
 */
public final class MVNativeBridge {

    private static final MethodHandle MV_INIT_RUNTIME;
    private static final MethodHandle MV_BUILD_ARROW;
    private static final MethodHandle MV_WRITER_CREATE;
    private static final MethodHandle MV_WRITER_FEED;
    private static final MethodHandle MV_WRITER_FINALIZE;
    private static final MethodHandle MV_WRITER_FINALIZE_ARROW;
    private static final MethodHandle MV_WRITER_ABORT;
    private static final MethodHandle MV_BUILD_MANAGED;
    private static final MethodHandle MV_BUILD_ARROW_MANAGED;
    private static final MethodHandle MV_ALLOC_CANCEL_CTX;
    private static final MethodHandle MV_RELEASE_CANCEL_CTX;
    private static final MethodHandle MV_CANCEL_BUILD;
    private static final MethodHandle MV_SEARCH_V2;
    private static final MethodHandle MV_STATE_FIELD_NAMES;
    private static final MethodHandle SORTED_PARQUET_MERGE;
    private static final MethodHandle MV_BUILD_STREAMING_RESULT;
    private static final MethodHandle MV_BUILD_RESULT_ABI_VERSION;
    private static final MethodHandle MV_CREATE_GLOBAL_RUNTIME;
    private static final MethodHandle MV_CLOSE_GLOBAL_RUNTIME;
    private static final MethodHandle MV_VALIDATE_DEFINITION;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        // Real-node finding: each plugin classloader loads ITS OWN native
        // instance (separate globals). The MV writers therefore need the
        // runtime manager initialized in THIS instance — the DataFusion
        // plugin's init lives in a different one. POC-grade; production
        // consolidates on one shared native instance.
        MV_INIT_RUNTIME = linker.downcallHandle(
            lib.find("df_init_runtime_manager").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT, ValueLayout.JAVA_DOUBLE, ValueLayout.JAVA_DOUBLE)
        );
        MV_BUILD_ARROW = linker.downcallHandle(
            lib.find("df_mv_build_arrow").orElseThrow(() -> new IllegalStateException("df_mv_build_arrow not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );
        // i64 df_mv_state_field_names(file_ptr, file_len, out_ptr, out_cap, out_len)
        MV_STATE_FIELD_NAMES = linker.downcallHandle(
            lib.find("df_mv_state_field_names").orElseThrow(() -> new IllegalStateException("df_mv_state_field_names not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        // i64 df_sorted_parquet_merge(runtime_ptr, files_ptr, files_len,
        // sort_cols_ptr, sort_cols_len, desc_ptr, nulls_first_ptr, sort_len,
        // output_ptr, output_len) — generic N-sorted-files -> one-sorted-file merge.
        SORTED_PARQUET_MERGE = linker.downcallHandle(
            lib.find("df_sorted_parquet_merge").orElseThrow(() -> new IllegalStateException("df_sorted_parquet_merge not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,        // runtime_ptr
                ValueLayout.ADDRESS,          // files_ptr (newline-joined)
                ValueLayout.JAVA_LONG,        // files_len
                ValueLayout.ADDRESS,          // sort_cols_ptr (newline-joined)
                ValueLayout.JAVA_LONG,        // sort_cols_len
                ValueLayout.ADDRESS,          // desc_ptr (int[] flags)
                ValueLayout.ADDRESS,          // nulls_first_ptr (int[] flags)
                ValueLayout.JAVA_INT,         // sort_len
                ValueLayout.ADDRESS,          // output_ptr
                ValueLayout.JAVA_LONG         // output_len
            )
        );
        MV_WRITER_CREATE = linker.downcallHandle(
            lib.find("df_mv_writer_create").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        MV_WRITER_FEED = linker.downcallHandle(
            lib.find("df_mv_writer_feed").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        MV_WRITER_FINALIZE = linker.downcallHandle(
            lib.find("df_mv_writer_finalize").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        // i64 df_mv_writer_finalize_arrow(writer_id, array_addr, schema_addr)
        MV_WRITER_FINALIZE_ARROW = linker.downcallHandle(
            lib.find("df_mv_writer_finalize_arrow").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        MV_WRITER_ABORT = linker.downcallHandle(
            lib.find("df_mv_writer_abort").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
        MV_SEARCH_V2 = linker.downcallHandle(
            lib.find("df_mv_search_v2").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        // Stage 2: managed build through shared DataFusionRuntime
        // i64 df_mv_build_managed(runtime_ptr, input_ptr, input_len, table_ptr, table_len,
        // sql_ptr, sql_len, output_ptr, output_len,
        // ordering_indices_ptr, ordering_dirs_ptr, ordering_nulls_ptr, ordering_len,
        // context_id, spill_budget_bytes, spill_file_count_limit)
        MV_BUILD_MANAGED = linker.downcallHandle(
            lib.find("df_mv_build_managed").orElseThrow(() -> new IllegalStateException("df_mv_build_managed not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,       // runtime_ptr
                ValueLayout.ADDRESS,          // input_ptr
                ValueLayout.JAVA_LONG,        // input_len
                ValueLayout.ADDRESS,          // table_ptr
                ValueLayout.JAVA_LONG,        // table_len
                ValueLayout.ADDRESS,          // sql_ptr
                ValueLayout.JAVA_LONG,        // sql_len
                ValueLayout.ADDRESS,          // output_ptr
                ValueLayout.JAVA_LONG,        // output_len
                ValueLayout.ADDRESS,          // ordering_indices_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_dirs_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_nulls_ptr (int[])
                ValueLayout.JAVA_INT,         // ordering_len
                ValueLayout.JAVA_LONG,        // context_id
                ValueLayout.JAVA_LONG,        // spill_budget_bytes
                ValueLayout.JAVA_INT          // spill_file_count_limit
            )
        );
        // i64 df_mv_build_arrow_managed(runtime_ptr, input_ptr, input_len, table_ptr, table_len,
        // sql_ptr, sql_len, array_addr, schema_addr,
        // ordering_indices_ptr, ordering_dirs_ptr, ordering_nulls_ptr, ordering_len,
        // context_id, spill_budget_bytes, spill_file_count_limit)
        MV_BUILD_ARROW_MANAGED = linker.downcallHandle(
            lib.find("df_mv_build_arrow_managed").orElseThrow(() -> new IllegalStateException("df_mv_build_arrow_managed not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,       // runtime_ptr
                ValueLayout.ADDRESS,          // input_ptr
                ValueLayout.JAVA_LONG,        // input_len
                ValueLayout.ADDRESS,          // table_ptr
                ValueLayout.JAVA_LONG,        // table_len
                ValueLayout.ADDRESS,          // sql_ptr
                ValueLayout.JAVA_LONG,        // sql_len
                ValueLayout.JAVA_LONG,        // array_addr
                ValueLayout.JAVA_LONG,        // schema_addr
                ValueLayout.ADDRESS,          // ordering_indices_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_dirs_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_nulls_ptr (int[])
                ValueLayout.JAVA_INT,         // ordering_len
                ValueLayout.JAVA_LONG,        // context_id
                ValueLayout.JAVA_LONG,        // spill_budget_bytes
                ValueLayout.JAVA_INT          // spill_file_count_limit
            )
        );
        // i64 df_mv_alloc_cancel_ctx() -> context_id
        MV_ALLOC_CANCEL_CTX = linker.downcallHandle(
            lib.find("df_mv_alloc_cancel_ctx").orElseThrow(() -> new IllegalStateException("df_mv_alloc_cancel_ctx not found")),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        );
        // void df_mv_release_cancel_ctx(context_id)
        MV_RELEASE_CANCEL_CTX = linker.downcallHandle(
            lib.find("df_mv_release_cancel_ctx").orElseThrow(() -> new IllegalStateException("df_mv_release_cancel_ctx not found")),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
        // void df_mv_cancel_build(context_id)
        MV_CANCEL_BUILD = linker.downcallHandle(
            lib.find("df_mv_cancel_build").orElseThrow(() -> new IllegalStateException("df_mv_cancel_build not found")),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
        // Stage 3: Streaming build with full MvBuildResult struct output.
        // i64 df_mv_build_streaming_result(runtime_ptr,
        //   input_ptr, input_len, table_ptr, table_len, sql_ptr, sql_len,
        //   output_ptr, output_len,
        //   ordering_indices_ptr, ordering_dirs_ptr, ordering_nulls_ptr, ordering_len,
        //   context_id, spill_budget_bytes, spill_file_count_limit,
        //   out_result_ptr)
        MV_BUILD_STREAMING_RESULT = linker.downcallHandle(
            lib.find("df_mv_build_streaming_result")
                .orElseThrow(() -> new IllegalStateException("df_mv_build_streaming_result not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,       // runtime_ptr
                ValueLayout.ADDRESS,          // input_ptr
                ValueLayout.JAVA_LONG,        // input_len
                ValueLayout.ADDRESS,          // table_ptr
                ValueLayout.JAVA_LONG,        // table_len
                ValueLayout.ADDRESS,          // sql_ptr
                ValueLayout.JAVA_LONG,        // sql_len
                ValueLayout.ADDRESS,          // output_ptr
                ValueLayout.JAVA_LONG,        // output_len
                ValueLayout.ADDRESS,          // ordering_indices_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_dirs_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_nulls_ptr (int[])
                ValueLayout.JAVA_INT,         // ordering_len
                ValueLayout.JAVA_LONG,        // context_id
                ValueLayout.JAVA_LONG,        // spill_budget_bytes
                ValueLayout.JAVA_INT,         // spill_file_count_limit
                ValueLayout.ADDRESS           // out_result_ptr (MvBuildResult*)
            )
        );
        // u32 df_mv_build_result_abi_version() — sanity check at load time
        MV_BUILD_RESULT_ABI_VERSION = linker.downcallHandle(
            lib.find("df_mv_build_result_abi_version")
                .orElseThrow(() -> new IllegalStateException("df_mv_build_result_abi_version not found")),
            FunctionDescriptor.of(ValueLayout.JAVA_INT)
        );
        // Stage 2: create/close a shared DataFusionRuntime within this native instance
        // i64 df_create_global_runtime(memory_pool_limit, cache_manager_ptr, spill_dir_ptr, spill_dir_len, spill_limit)
        MV_CREATE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_create_global_runtime").orElseThrow(() -> new IllegalStateException("df_create_global_runtime not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,        // memory_pool_limit
                ValueLayout.JAVA_LONG,        // cache_manager_ptr (0 = none)
                ValueLayout.ADDRESS,          // spill_dir_ptr
                ValueLayout.JAVA_LONG,        // spill_dir_len
                ValueLayout.JAVA_LONG         // spill_limit
            )
        );
        // void df_close_global_runtime(ptr)
        MV_CLOSE_GLOBAL_RUNTIME = linker.downcallHandle(
            lib.find("df_close_global_runtime").orElseThrow(() -> new IllegalStateException("df_close_global_runtime not found")),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
        // Stage 3: native schema cross-check for a candidate MV definition.
        // i64 df_mv_validate_definition(schema_ptr, schema_len, table_ptr, table_len,
        //   sql_ptr, sql_len, ordering_indices_ptr, ordering_dirs_ptr, ordering_nulls_ptr,
        //   ordering_len, out_ptr, out_cap, out_len)
        MV_VALIDATE_DEFINITION = linker.downcallHandle(
            lib.find("df_mv_validate_definition")
                .orElseThrow(() -> new IllegalStateException("df_mv_validate_definition not found")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,          // schema_ptr (newline/tab source schema)
                ValueLayout.JAVA_LONG,        // schema_len
                ValueLayout.ADDRESS,          // table_ptr
                ValueLayout.JAVA_LONG,        // table_len
                ValueLayout.ADDRESS,          // sql_ptr
                ValueLayout.JAVA_LONG,        // sql_len
                ValueLayout.ADDRESS,          // ordering_indices_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_dirs_ptr (int[])
                ValueLayout.ADDRESS,          // ordering_nulls_ptr (int[])
                ValueLayout.JAVA_INT,         // ordering_len
                ValueLayout.ADDRESS,          // out_ptr
                ValueLayout.JAVA_LONG,        // out_cap
                ValueLayout.ADDRESS           // out_len
            )
        );
    }

    private MVNativeBridge() {}

    /** Initializes this native instance's tokio runtime manager (idempotent per instance). */
    public static void initRuntime(int cpuThreads) {
        try {
            MV_INIT_RUNTIME.invokeExact(cpuThreads, 1.0d, 1.0d);
        } catch (Throwable t) {
            throw new RuntimeException("df_init_runtime_manager failed", t);
        }
    }

    /**
     * Stage 2: Create a shared DataFusionRuntime within this native instance.
     * Returns a pointer to the runtime that can be passed to managed build
     * methods. The caller owns the runtime and must call
     * {@link #closeGlobalRuntime(long)} when done.
     *
     * @param memoryPoolLimit max bytes for the DataFusion memory pool
     * @param spillDir        directory for disk spill (empty = spill disabled)
     * @param spillLimit      max bytes for disk spill
     * @return native runtime pointer (non-zero on success)
     */
    public static long createGlobalRuntime(long memoryPoolLimit, String spillDir, long spillLimit) {
        try (var call = new NativeCall()) {
            var dir = call.str(spillDir != null ? spillDir : "");
            return call.invoke(
                MV_CREATE_GLOBAL_RUNTIME,
                memoryPoolLimit,
                0L, // no cache manager for MV builds
                dir.segment(),
                dir.len(),
                spillLimit
            );
        }
    }

    /**
     * Stage 2: Close a shared DataFusionRuntime. Must be called when the node
     * shuts down to free native resources.
     */
    public static void closeGlobalRuntime(long runtimePtr) {
        if (runtimePtr != 0) {
            try {
                MV_CLOSE_GLOBAL_RUNTIME.invokeExact(runtimePtr);
            } catch (Throwable t) {
                throw new RuntimeException("df_close_global_runtime failed", t);
            }
        }
    }

    // ── Stage 4: Streaming merge engine ──────────────────────────────────

    /**
     * Generic sorted-parquet merge on the shared DataFusionRuntime: merges N
     * individually sorted parquet files into ONE sorted file via a streaming
     * k-way SortPreservingMerge. Rows are preserved verbatim — no folding, no
     * format- or definition-specific semantics (the Lucene merge idiom).
     *
     * @param runtimePtr  shared DataFusionRuntime pointer (from createGlobalRuntime)
     * @param inputFiles  paths of the sorted input parquet files
     * @param sortColumns sort column names (the files' declared order)
     * @param descending  per-column descending flags (parallel to sortColumns)
     * @param nullsFirst  per-column nulls-first flags (parallel to sortColumns)
     * @param outputFile  path of the merged output parquet file
     * @return rows written to the merged file
     */
    public static long sortedParquetMerge(
        long runtimePtr,
        java.util.List<String> inputFiles,
        String[] sortColumns,
        boolean[] descending,
        boolean[] nullsFirst,
        String outputFile
    ) {
        try (var call = new NativeCall()) {
            var files = call.str(String.join("\n", inputFiles));
            var cols = call.str(String.join("\n", sortColumns));
            var desc = call.intArray(toIntArray(descending));
            var nulls = call.intArray(toIntArray(nullsFirst));
            var out = call.str(outputFile);
            return call.invoke(
                SORTED_PARQUET_MERGE,
                runtimePtr,
                files.segment(),
                files.len(),
                cols.segment(),
                cols.len(),
                desc.segment(),
                nulls.segment(),
                sortColumns.length,
                out.segment(),
                out.len()
            );
        }
    }

    /** Boolean flags -> 0/1 int array for FFI transport. */
    private static int[] toIntArray(boolean[] booleans) {
        int[] result = new int[booleans.length];
        for (int i = 0; i < booleans.length; i++) {
            result[i] = booleans[i] ? 1 : 0;
        }
        return result;
    }

    /**
     * Refresh-time ship build: Partial over one parquet file, sorted state
     * batch exported via Arrow C-Data into the given FFI struct addresses.
     * Returns the state row count.
     */
    public static long buildArrow(String inputFile, String tableName, String sql, long arrayAddr, long schemaAddr) {
        try (var call = new NativeCall()) {
            var in = call.str(inputFile);
            var table = call.str(tableName);
            var query = call.str(sql);
            return call.invoke(
                MV_BUILD_ARROW,
                in.segment(),
                in.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                arrayAddr,
                schemaAddr
            );
        }
    }

    /**
     * Reads the physical field names of a Parquet MV state file (footer only),
     * in physical order — the ground truth for the merge ordering identity.
     */
    public static java.util.List<String> stateFieldNames(String stateFile) {
        try (var call = new NativeCall()) {
            var file = call.str(stateFile);
            var out = call.outBuffer(64 * 1024);
            call.invoke(MV_STATE_FIELD_NAMES, file.segment(), file.len(), out.data(), (long) out.capacity(), out.lenOut());
            String joined = new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
            return joined.isEmpty() ? java.util.List.of() : java.util.List.of(joined.split("\n", -1));
        }
    }

    // ---- Streaming writer lifecycle (VSR model) ----

    public static long writerCreate(String definitionSql, int numGroupCols) {
        try (var call = new NativeCall()) {
            var sql = call.str(definitionSql);
            return call.invoke(MV_WRITER_CREATE, sql.segment(), sql.len(), (long) numGroupCols);
        }
    }

    public static void writerFeed(long writerId, long arrayAddress, long schemaAddress) {
        try (var call = new NativeCall()) {
            call.invoke(MV_WRITER_FEED, writerId, arrayAddress, schemaAddress);
        }
    }

    /**
     * Finalizes the writer and exports the sorted state batch via Arrow C-Data
     * into the given caller-allocated struct addresses — zero copy; the caller
     * imports the structs exactly once and owns the resulting root. Returns
     * the state row count.
     */
    public static long writerFinalizeArrow(long writerId, long arrayAddress, long schemaAddress) {
        try (var call = new NativeCall()) {
            return call.invoke(MV_WRITER_FINALIZE_ARROW, writerId, arrayAddress, schemaAddress);
        }
    }

    public static long writerFinalize(long writerId, String outputFile) {
        try (var call = new NativeCall()) {
            var out = call.str(outputFile);
            return call.invoke(MV_WRITER_FINALIZE, writerId, out.segment(), out.len());
        }
    }

    /** v2 search: SQL template with __MV_STATES__ placeholder over the state files. */
    public static String searchV2(java.util.List<String> stateFiles, String sqlTemplate) {
        try (var call = new NativeCall()) {
            var files = call.strArray(stateFiles.toArray(new String[0]));
            var sql = call.str(sqlTemplate);
            var out = call.outBuffer(1024 * 1024);
            call.invoke(
                MV_SEARCH_V2,
                files.ptrs(),
                files.lens(),
                (long) stateFiles.size(),
                sql.segment(),
                sql.len(),
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }

    public static void writerAbort(long writerId) {
        // Void native call — NativeCall.invoke expects a long return; invoke directly.
        try {
            MV_WRITER_ABORT.invokeExact(writerId);
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_writer_abort failed", t);
        }
    }

    // ── Stage 2: Managed build through shared DataFusionRuntime ──────────

    /**
     * Allocate a cancellation context for an MV build. Returns a context_id
     * that can be passed to {@link #cancelBuild(long)}.
     */
    public static long allocateCancellationContext() {
        try {
            return (long) MV_ALLOC_CANCEL_CTX.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_alloc_cancel_ctx failed", t);
        }
    }

    /**
     * Release a cancellation context. Must be called after the build completes.
     */
    public static void releaseCancellationContext(long contextId) {
        try {
            MV_RELEASE_CANCEL_CTX.invokeExact(contextId);
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_release_cancel_ctx failed", t);
        }
    }

    /**
     * Fire the cancellation token for an in-flight MV build.
     */
    public static void cancelBuild(long contextId) {
        try {
            MV_CANCEL_BUILD.invokeExact(contextId);
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_cancel_build failed", t);
        }
    }

    /**
     * Stage 2 managed state-file build: runs through the shared DataFusionRuntime
     * with full ordering contract, cancellation support, and spill budget.
     *
     * @param runtimePtr         shared DataFusionRuntime pointer
     * @param inputFile          staged parquet directory
     * @param tableName          DataFusion table name
     * @param sql                filtered SQL
     * @param outputFile         output Arrow IPC state file path
     * @param orderingIndices    state-field indices for lexsort (from groupByOrdering)
     * @param orderingDirs       direction wire tokens (0=ASC)
     * @param orderingNulls      null-placement wire tokens (0=NULLS_FIRST, 1=NULLS_LAST)
     * @param contextId          cancellation context id
     * @param spillBudgetBytes   per-build spill byte limit (0 = global)
     * @param spillFileCountLimit per-build spill file count limit (0 = unlimited)
     * @return state row count
     */
    public static long buildStateFileManaged(
        long runtimePtr,
        String inputFile,
        String tableName,
        String sql,
        String outputFile,
        int[] orderingIndices,
        int[] orderingDirs,
        int[] orderingNulls,
        long contextId,
        long spillBudgetBytes,
        int spillFileCountLimit
    ) {
        try (var call = new NativeCall()) {
            var in_ = call.str(inputFile);
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.str(outputFile);
            var indices = call.intArray(orderingIndices);
            var dirs = call.intArray(orderingDirs);
            var nulls = call.intArray(orderingNulls);
            return call.invoke(
                MV_BUILD_MANAGED,
                runtimePtr,
                in_.segment(),
                in_.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                out.segment(),
                out.len(),
                indices.segment(),
                dirs.segment(),
                nulls.segment(),
                orderingIndices.length,
                contextId,
                spillBudgetBytes,
                spillFileCountLimit
            );
        }
    }

    /**
     * Stage 2 managed Arrow C-Data build: same as managed state file but exports
     * via Arrow C-Data instead of writing to disk.
     */
    public static long buildArrowManaged(
        long runtimePtr,
        String inputFile,
        String tableName,
        String sql,
        long arrayAddr,
        long schemaAddr,
        int[] orderingIndices,
        int[] orderingDirs,
        int[] orderingNulls,
        long contextId,
        long spillBudgetBytes,
        int spillFileCountLimit
    ) {
        try (var call = new NativeCall()) {
            var in_ = call.str(inputFile);
            var table = call.str(tableName);
            var query = call.str(sql);
            var indices = call.intArray(orderingIndices);
            var dirs = call.intArray(orderingDirs);
            var nulls = call.intArray(orderingNulls);
            return call.invoke(
                MV_BUILD_ARROW_MANAGED,
                runtimePtr,
                in_.segment(),
                in_.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                arrayAddr,
                schemaAddr,
                indices.segment(),
                dirs.segment(),
                nulls.segment(),
                orderingIndices.length,
                contextId,
                spillBudgetBytes,
                spillFileCountLimit
            );
        }
    }

    // ── Stage 3: Streaming build with MvBuildResult output ───────────

    /**
     * Query the native library for the MvBuildResult ABI version.
     * Called once at bridge init to validate Java/Rust contract parity.
     *
     * @return the ABI version constant from the native library
     */
    public static int getResultAbiVersion() {
        try {
            return (int) MV_BUILD_RESULT_ABI_VERSION.invokeExact();
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_build_result_abi_version failed", t);
        }
    }

    /**
     * Stage 3 streaming build through the shared DataFusionRuntime that
     * returns a full {@code MvBuildResult} struct via an out-pointer. The
     * caller allocates an 80+ byte {@link MemorySegment} buffer, passes it
     * as the last parameter, and decodes the result via
     * {@link org.opensearch.mv.pull.MvBuildResultLayout}.
     *
     * <p>On success the native function returns 0 and writes the struct into
     * {@code outResultBuf}. On internal error it returns a negative error
     * pointer (standard {@code checkResult} convention).</p>
     *
     * @param runtimePtr         shared DataFusionRuntime pointer
     * @param inputFile          staged parquet directory
     * @param tableName          DataFusion table name
     * @param sql                filtered SQL
     * @param outputFile         output Arrow IPC state file path
     * @param orderingIndices    state-field indices for lexsort
     * @param orderingDirs       direction wire tokens (0=ASC)
     * @param orderingNulls      null-placement wire tokens (0=NULLS_FIRST)
     * @param contextId          cancellation context id
     * @param spillBudgetBytes   per-build spill byte limit
     * @param spillFileCountLimit per-build spill file count limit
     * @param outResultBuf       caller-allocated buffer (≥80 bytes) for MvBuildResult
     */
    public static void buildStreamingArtifactNative(
        long runtimePtr,
        String inputFile,
        String tableName,
        String sql,
        String outputFile,
        int[] orderingIndices,
        int[] orderingDirs,
        int[] orderingNulls,
        long contextId,
        long spillBudgetBytes,
        int spillFileCountLimit,
        MemorySegment outResultBuf
    ) {
        try (var call = new NativeCall()) {
            var in_ = call.str(inputFile);
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.str(outputFile);
            var indices = call.intArray(orderingIndices);
            var dirs = call.intArray(orderingDirs);
            var nulls = call.intArray(orderingNulls);
            call.invoke(
                MV_BUILD_STREAMING_RESULT,
                runtimePtr,
                in_.segment(),
                in_.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                out.segment(),
                out.len(),
                indices.segment(),
                dirs.segment(),
                nulls.segment(),
                orderingIndices.length,
                contextId,
                spillBudgetBytes,
                spillFileCountLimit,
                outResultBuf
            );
        }
    }

    // ── Stage 3: Native schema cross-check (definition validation) ───────

    /**
     * Stage 3 native cross-check: plan (never execute) a candidate MV
     * definition's canonical partial SQL + ordering contract against the REAL
     * source Arrow schema and return the engine's ACTUAL Partial-stage state
     * schema + deterministic hashes as a small text document.
     *
     * <p>This is the thin FFI binding; the fail-closed comparison against the
     * descriptor-derived expectation lives in
     * {@link MVDefinitionValidator}. The returned text is parsed there.</p>
     *
     * <p>Wire encodings (see {@code df_mv_validate_definition}):</p>
     * <ul>
     *   <li><b>Input</b> {@code sourceSchema}: newline-separated
     *       {@code name\tarrow_token} records.</li>
     *   <li><b>Output</b> (returned string): newline-separated records —
     *       {@code schema_hash\t<u64>}, {@code ordering_identity_hash\t<u64>},
     *       {@code definition_hash\t<u64>}, then one
     *       {@code field\t<name>\t<arrow_token>} per state column in physical
     *       order.</li>
     * </ul>
     *
     * @param sourceSchema     newline/tab-encoded source schema (arrow tokens)
     * @param tableName        DataFusion table name the SQL is written against
     *                         (canonically {@link MVConstants#INPUT_TABLE})
     * @param sql              canonical partial SQL
     * @param orderingIndices  state-field indices for the sort key
     * @param orderingDirs     direction wire tokens (0=ASC)
     * @param orderingNulls    null-placement wire tokens (0=NULLS_FIRST)
     * @return the native result text (see above)
     * @throws RuntimeException if the definition is rejected (unknown column,
     *                          unparseable SQL, type mismatch, bad schema) —
     *                          the message is the precise native error
     */
    public static String validateDefinition(
        String sourceSchema,
        String tableName,
        String sql,
        int[] orderingIndices,
        int[] orderingDirs,
        int[] orderingNulls
    ) {
        try (var call = new NativeCall()) {
            var schema = call.str(sourceSchema);
            var table = call.str(tableName);
            var query = call.str(sql);
            var indices = call.intArray(orderingIndices);
            var dirs = call.intArray(orderingDirs);
            var nulls = call.intArray(orderingNulls);
            var out = call.outBuffer(256 * 1024);
            call.invoke(
                MV_VALIDATE_DEFINITION,
                schema.segment(),
                schema.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                indices.segment(),
                dirs.segment(),
                nulls.segment(),
                orderingIndices.length,
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
        }
    }
}
