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
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * POC(mv): minimal FFI binding for the MV state-file build. Binds
 * {@code df_mv_build_poc} from the shared native lib directly — no dependency
 * on the DataFusion plugin's Java classes (avoids cross-plugin classloader
 * coupling; both crates' symbols live in the one shared .so).
 */
public final class MVNativeBridge {

    private static final MethodHandle MV_BUILD_POC;
    private static final MethodHandle MV_SEARCH_POC;
    private static final MethodHandle MV_WRITER_CREATE;
    private static final MethodHandle MV_WRITER_FEED;
    private static final MethodHandle MV_WRITER_FINALIZE;
    private static final MethodHandle MV_WRITER_ABORT;
    private static final MethodHandle MV_SEARCH_V2;

    static {
        Linker linker = Linker.nativeLinker();
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        // i64 df_mv_build_poc(input_ptr, input_len, table_ptr, table_len, sql_ptr, sql_len, output_ptr, output_len)
        MV_BUILD_POC = linker.downcallHandle(
            lib.find("df_mv_build_poc").orElseThrow(() -> new IllegalStateException("df_mv_build_poc symbol missing")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG
            )
        );
        // i64 df_mv_search_poc(files_ptr, files_lens, files_count, group_ptr, group_len,
        // state_ptr, state_len, out_ptr, out_cap, out_len)
        MV_SEARCH_POC = linker.downcallHandle(
            lib.find("df_mv_search_poc").orElseThrow(() -> new IllegalStateException("df_mv_search_poc symbol missing")),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
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
    }

    private MVNativeBridge() {}

    /**
     * Blocking MV state-file build: reads {@code inputFile} (primary parquet),
     * runs {@code sql} stopped at Partial mode, writes state rows (sorted, per
     * the ORDER BY in the sql) to {@code outputFile}. Returns state row count.
     */
    public static long buildStateFile(String inputFile, String tableName, String sql, String outputFile) {
        try (var call = new NativeCall()) {
            var in = call.str(inputFile);
            var table = call.str(tableName);
            var query = call.str(sql);
            var out = call.str(outputFile);
            return call.invoke(
                MV_BUILD_POC,
                in.segment(),
                in.len(),
                table.segment(),
                table.len(),
                query.segment(),
                query.len(),
                out.segment(),
                out.len()
            );
        }
    }

    /**
     * POC search: Final-aggregation over the given MV state files. Always goes
     * to the MV (no fallback). Returns tab-separated "group\tcount" lines.
     */
    public static String search(java.util.List<String> stateFiles, String groupKey, String stateCol) {
        try (var call = new NativeCall()) {
            var files = call.strArray(stateFiles.toArray(new String[0]));
            var group = call.str(groupKey);
            var state = call.str(stateCol);
            var out = call.outBuffer(1024 * 1024);
            call.invoke(
                MV_SEARCH_POC,
                files.ptrs(),
                files.lens(),
                (long) stateFiles.size(),
                group.segment(),
                group.len(),
                state.segment(),
                state.len(),
                out.data(),
                (long) out.capacity(),
                out.lenOut()
            );
            return new String(out.toByteArray(), java.nio.charset.StandardCharsets.UTF_8);
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
}
