/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM (Foreign Function and Memory) bridge for MV native operations.
 * Thin wrapper over {@code df_mv_query_state} in the DataFusion native library.
 *
 * <p>The native library is loaded by the analytics-backend-datafusion plugin's
 * existing native init path. This class only looks up the symbol — it does not
 * load the library itself.</p>
 *
 * <p>Production path note: in a full integration, these bindings would live in
 * the analytics-backend-datafusion Java layer alongside NativeBridge.java.
 * For POC isolation, they're kept in mv-engine to avoid modifying the
 * production NativeBridge.</p>
 *
 * @opensearch.internal
 */
public final class MVNativeBridge {

    private static final Logger logger = LogManager.getLogger(MVNativeBridge.class);

    /** Arrow C-Data struct sizes (conservative upper bounds for FFM allocation). */
    public static final long ARROW_ARRAY_SIZE = 256;
    public static final long ARROW_SCHEMA_SIZE = 256;

    private static volatile MethodHandle MH_QUERY_STATE;

    private MVNativeBridge() {}

    /**
     * Initialize the FFM method handles. Called once from MVEnginePlugin.createComponents
     * after the native library is loaded by the analytics-backend-datafusion plugin.
     */
    public static void init() {
        try {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = SymbolLookup.loaderLookup();

            // df_mv_query_state(dirs_ptr, dirs_len, sql_ptr, sql_len, schema_ptr, schema_len, out_array, out_schema) -> i64
            var queryOpt = lookup.find("df_mv_query_state");
            if (queryOpt.isPresent()) {
                MH_QUERY_STATE = linker.downcallHandle(
                    queryOpt.get(),
                    FunctionDescriptor.of(
                        ValueLayout.JAVA_LONG,    // return: rows or error
                        ValueLayout.ADDRESS,      // dirs_json_ptr
                        ValueLayout.JAVA_LONG,    // dirs_json_len
                        ValueLayout.ADDRESS,      // def_sql_ptr
                        ValueLayout.JAVA_LONG,    // def_sql_len
                        ValueLayout.ADDRESS,      // schema_json_ptr
                        ValueLayout.JAVA_LONG,    // schema_json_len
                        ValueLayout.JAVA_LONG,    // out_array_addr
                        ValueLayout.JAVA_LONG     // out_schema_addr
                    )
                );
                logger.info("MVNativeBridge: df_mv_query_state bound");
            } else {
                logger.warn("MVNativeBridge: df_mv_query_state symbol not found — MV query disabled");
            }
        } catch (Exception e) {
            logger.error("MVNativeBridge: failed to init FFM bindings", e);
        }
    }

    /**
     * Query hydrated MV state files via native DataFusion Final aggregation.
     *
     * @param dirsNative   native memory segment with JSON array of directory paths
     * @param dirsLen      length of dirs JSON in bytes
     * @param sqlNative    native memory segment with definition SQL
     * @param sqlLen       length of SQL in bytes
     * @param schemaNative native memory segment with Arrow schema JSON
     * @param schemaLen    length of schema JSON in bytes
     * @param outArrayAddr address for Arrow C-Data array export
     * @param outSchemaAddr address for Arrow C-Data schema export
     * @return number of result rows, or throws on error
     */
    public static long queryState(
        MemorySegment dirsNative, long dirsLen,
        MemorySegment sqlNative, long sqlLen,
        MemorySegment schemaNative, long schemaLen,
        long outArrayAddr, long outSchemaAddr
    ) {
        if (MH_QUERY_STATE == null) {
            throw new UnsupportedOperationException("df_mv_query_state not available — native library not loaded");
        }
        try {
            long result = (long) MH_QUERY_STATE.invokeExact(
                dirsNative, dirsLen,
                sqlNative, sqlLen,
                schemaNative, schemaLen,
                outArrayAddr, outSchemaAddr
            );
            if (result < 0) {
                throw new RuntimeException("df_mv_query_state returned error code: " + result);
            }
            return result;
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (Throwable t) {
            throw new RuntimeException("df_mv_query_state invocation failed", t);
        }
    }
}
