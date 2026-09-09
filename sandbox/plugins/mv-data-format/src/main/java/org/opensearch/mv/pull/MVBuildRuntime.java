/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVGroupByOrdering;
import org.opensearch.mv.MVNativeBridge;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stage 2: Routes all production pull-path MV builds through the shared
 * DataFusion analytics runtime (memory pool, disk spill, cache) instead of
 * POC-grade private {@code SessionContext::new_with_state} runtimes.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>Serialize the {@link MVGroupByOrdering} contract across FFI to Rust</li>
 *   <li>Delegate builds to the shared {@code DataFusionRuntime} via new FFI entrypoints</li>
 *   <li>Enforce spill budget (bytes limit, file count limit)</li>
 *   <li>Propagate cancellation tokens through DataFusion execution</li>
 *   <li>Account memory reservations against the OpenSearch circuit breaker</li>
 * </ul>
 *
 * <p>The FFI contract carries the full ordering tuple from
 * {@link MVGroupByOrdering}: field indices, direction wire tokens, and
 * null-placement wire tokens. Rust uses these directly for
 * {@code lexsort_to_indices} instead of the hardcoded {@code column(0)}.
 */
public final class MVBuildRuntime implements Closeable {

    private static final Logger logger = LogManager.getLogger(MVBuildRuntime.class);

    // ── Spill budget settings ────────────────────────────────────────────

    /** Maximum bytes the MV build may spill to disk (0 = inherit from global). */
    public static final Setting<Long> MV_SPILL_BUDGET_BYTES = Setting.longSetting(
        "index.mv_pull.spill_budget_bytes",
        0L,
        0L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /** Maximum number of spill files the MV build may create (0 = unlimited). */
    public static final Setting<Integer> MV_SPILL_FILE_COUNT_LIMIT = Setting.intSetting(
        "index.mv_pull.spill_file_count_limit",
        0,
        0,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    /**
     * Estimated memory reservation (bytes) per MV build for circuit breaker
     * accounting. This is a pre-flight reservation claimed before the native
     * build starts and released after it completes, ensuring the OpenSearch
     * parent circuit breaker can reject new builds before OOM.
     *
     * <p>Default: 64 MiB. Operators can tune per-index if definitions have
     * widely different group cardinalities.
     */
    public static final Setting<Long> MV_BUILD_MEMORY_ESTIMATE = Setting.longSetting(
        "index.mv_pull.build_memory_estimate_bytes",
        64L * 1024 * 1024,
        0L,
        Setting.Property.IndexScope,
        Setting.Property.Dynamic
    );

    // ── Instance state ───────────────────────────────────────────────────

    private final long runtimePtr;
    private final long spillBudgetBytes;
    private final int spillFileCountLimit;
    private final long buildMemoryEstimate;
    private final CircuitBreaker circuitBreaker;
    private final AtomicLong activeContextId = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * @param runtimePtr           pointer to the shared DataFusionRuntime (from DataFusionService)
     * @param spillBudgetBytes     per-build spill byte limit (0 = global limit)
     * @param spillFileCountLimit  per-build spill file count limit (0 = unlimited)
     * @param buildMemoryEstimate  bytes to pre-reserve against the circuit breaker (0 = no reservation)
     * @param circuitBreaker       OpenSearch parent circuit breaker for memory accounting (may be null)
     */
    public MVBuildRuntime(
        long runtimePtr,
        long spillBudgetBytes,
        int spillFileCountLimit,
        long buildMemoryEstimate,
        CircuitBreaker circuitBreaker
    ) {
        if (runtimePtr == 0) {
            throw new IllegalArgumentException("runtimePtr must be non-zero (shared DataFusionRuntime)");
        }
        this.runtimePtr = runtimePtr;
        this.spillBudgetBytes = spillBudgetBytes;
        this.spillFileCountLimit = spillFileCountLimit;
        this.buildMemoryEstimate = buildMemoryEstimate;
        this.circuitBreaker = circuitBreaker;
    }

    /**
     * Convenience constructor without circuit breaker integration.
     */
    public MVBuildRuntime(long runtimePtr, long spillBudgetBytes, int spillFileCountLimit) {
        this(runtimePtr, spillBudgetBytes, spillFileCountLimit, 0, null);
    }

    /**
     * Stage 3: Build an MV state artifact using streaming external sort + direct IPC write.
     * No terminal collect/concat/sort/take in the production path.
     *
     * <p>The native side wraps the partial aggregation in a SortExec over the
     * FULL group-by ordering tuple, then streams sorted batches directly into
     * an Arrow IPC FileWriter. The shared DataFusionRuntime handles spillable
     * external sort via its DiskManager.</p>
     *
     * @param parquetInput    staged parquet directory
     * @param tableName       DataFusion table name
     * @param filteredSql     the definition SQL with seq-no range filter
     * @param outputFile      path for the output Arrow IPC state file
     * @param ordering        the full GROUP BY ordering contract
     * @return ArtifactResult containing row count, schema hash, and definition hash
     */
    public ArtifactResult buildStreamingArtifact(
        String parquetInput,
        String tableName,
        String filteredSql,
        String outputFile,
        MVGroupByOrdering ordering
    ) throws IOException {
        Objects.requireNonNull(ordering, "ordering");
        ensureOpen();

        reserveBreaker("buildStreamingArtifact");

        MVCompiledDefinition.OrderingFFIMetadata ffi = MVCompiledDefinition.OrderingFFIMetadata.from(ordering);
        long contextId = MVNativeBridge.allocateCancellationContext();
        activeContextId.set(contextId);

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment resultBuf = arena.allocate(MvBuildResultLayout.NATIVE_ALLOC_SIZE);

            // ── NATIVE_FFI_PRE log (instrumentation point 4 detail) ──
            logger.info(
                "mv_pull NATIVE_FFI_PRE runtimePtr={} spillBudget={}B spillFileLimit={} "
                    + "memEstimate={}B breaker_used={} breaker_limit={} ordering_fields={}",
                runtimePtr,
                spillBudgetBytes,
                spillFileCountLimit,
                buildMemoryEstimate,
                circuitBreaker != null ? circuitBreaker.getUsed() : -1,
                circuitBreaker != null ? circuitBreaker.getLimit() : -1,
                ffi.fieldIndices().length
            );

            MVNativeBridge.buildStreamingArtifactNative(
                runtimePtr,
                parquetInput,
                tableName,
                filteredSql,
                outputFile,
                ffi.fieldIndices(),
                ffi.directionTokens(),
                ffi.nullPlacementTokens(),
                contextId,
                spillBudgetBytes,
                spillFileCountLimit,
                resultBuf
            );

            // Validate ABI version and struct size
            MvBuildResultLayout.validate(resultBuf);

            int statusCode = MvBuildResultLayout.statusCode(resultBuf);

            // Handle non-OK status codes
            if (statusCode == MvBuildResultLayout.STATUS_CANCELLED) {
                MVBuildMetrics.INSTANCE.recordBuildFailed();
                throw new IOException("mv_pull streaming build was cancelled");
            }
            if (statusCode != MvBuildResultLayout.STATUS_OK) {
                MVBuildMetrics.INSTANCE.recordBuildFailed();
                throw new IOException(
                    "mv_pull streaming build failed with status_code=" + statusCode
                );
            }

            // Decode all fields from the native result
            long rows = MvBuildResultLayout.rowCount(resultBuf);
            long schemaHash = MvBuildResultLayout.schemaHash(resultBuf);
            long definitionHash = MvBuildResultLayout.definitionHash(resultBuf);
            long orderingHash = MvBuildResultLayout.orderingHash(resultBuf);
            long spillBytesVal = MvBuildResultLayout.spillBytes(resultBuf);
            int spillFileCountVal = MvBuildResultLayout.spillFileCount(resultBuf);
            int outputBatchCount = MvBuildResultLayout.outputBatchCount(resultBuf);
            long peakRssBytes = MvBuildResultLayout.peakRssBytes(resultBuf);
            long buildDurationUs = MvBuildResultLayout.buildDurationUs(resultBuf);

            if (rows <= 0L) {
                MVBuildMetrics.INSTANCE.recordBuildFailed();
                throw new IOException("mv_pull streaming build produced no state rows");
            }

            // ── NATIVE_FFI_POST log (instrumentation point 4 detail) ─
            logger.info(
                "mv_pull NATIVE_FFI_POST status={} rows={} spill_bytes={} spill_files={} "
                    + "output_batches={} peak_rss_bytes={} build_duration_us={} "
                    + "schema_hash={} definition_hash={} ordering_hash={}",
                statusCode,
                rows,
                spillBytesVal,
                spillFileCountVal,
                outputBatchCount,
                peakRssBytes,
                buildDurationUs,
                Long.toHexString(schemaHash),
                Long.toHexString(definitionHash),
                Long.toHexString(orderingHash)
            );

            // Validate ordering identity: compare native ordering hash against
            // the Java-computed ordering identity hash. Fail-closed on mismatch.
            long expectedOrderingHash = ordering.orderingIdentityHash();
            if (orderingHash != expectedOrderingHash) {
                MVBuildMetrics.INSTANCE.recordBuildFailed();
                throw new MvBuildOrderingMismatchException(
                    "Ordering identity hash mismatch after build: native="
                        + Long.toHexString(orderingHash)
                        + " java="
                        + Long.toHexString(expectedOrderingHash)
                        + ". The native library computed a different ordering contract."
                );
            }

            // Record spill metrics (real values from native instrumentation)
            MVBuildMetrics.INSTANCE.recordSpill(spillBytesVal, spillFileCountVal);
            MVBuildMetrics.INSTANCE.recordRss(peakRssBytes);
            MVBuildMetrics.INSTANCE.recordBuildDuration(buildDurationUs);
            MVBuildMetrics.INSTANCE.recordOutputBatches(outputBatchCount);
            MVBuildMetrics.INSTANCE.recordBuildCompleted();

            return new ArtifactResult(
                rows,
                schemaHash,
                definitionHash,
                orderingHash,
                spillBytesVal,
                spillFileCountVal,
                outputBatchCount,
                peakRssBytes,
                buildDurationUs,
                statusCode
            );
        } catch (MvBuildOrderingMismatchException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            MVBuildMetrics.INSTANCE.recordBuildFailed();
            throw new IOException("mv_pull streaming build failed", e);
        } finally {
            activeContextId.set(0);
            MVNativeBridge.releaseCancellationContext(contextId);
            releaseBreaker("buildStreamingArtifact");
        }
    }

    /**
     * Build an MV state artifact through the shared DataFusion runtime.
     *
     * @deprecated Use {@link #buildStreamingArtifact} for the production path.
     *             This method is retained for the Arrow C-Data export path and
     *             legacy callers that need a row count only.
     *
     * @param parquetInput    staged parquet directory
     * @param tableName       DataFusion table name
     * @param filteredSql     the definition SQL with seq-no range filter
     * @param outputFile      path for the output Arrow IPC state file
     * @param ordering        the full GROUP BY ordering contract
     * @return state row count
     */
    @Deprecated
    public long buildStateManaged(String parquetInput, String tableName, String filteredSql, String outputFile, MVGroupByOrdering ordering)
        throws IOException {
        Objects.requireNonNull(ordering, "ordering");
        ensureOpen();

        // Circuit breaker: pre-flight memory reservation
        reserveBreaker("buildStateManaged");

        // Serialize ordering contract for FFI
        MVCompiledDefinition.OrderingFFIMetadata ffi = MVCompiledDefinition.OrderingFFIMetadata.from(ordering);

        // Allocate a cancellation context id (unique per build)
        long contextId = MVNativeBridge.allocateCancellationContext();
        activeContextId.set(contextId);

        try {
            long rows = MVNativeBridge.buildStateFileManaged(
                runtimePtr,
                parquetInput,
                tableName,
                filteredSql,
                outputFile,
                ffi.fieldIndices(),
                ffi.directionTokens(),
                ffi.nullPlacementTokens(),
                contextId,
                spillBudgetBytes,
                spillFileCountLimit
            );
            if (rows <= 0L) {
                MVBuildMetrics.INSTANCE.recordBuildFailed();
                throw new IOException("mv_pull managed build produced no state rows");
            }
            MVBuildMetrics.INSTANCE.recordBuildCompleted();
            return rows;
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            MVBuildMetrics.INSTANCE.recordBuildFailed();
            throw new IOException("mv_pull managed build failed", e);
        } finally {
            activeContextId.set(0);
            MVNativeBridge.releaseCancellationContext(contextId);
            releaseBreaker("buildStateManaged");
        }
    }

    /**
     * Build and export via Arrow C-Data through the shared runtime.
     * Replaces POC {@code MVNativeBridge.buildArrow}.
     */
    public long buildArrowManaged(
        String parquetInput,
        String tableName,
        String filteredSql,
        long arrayAddr,
        long schemaAddr,
        MVGroupByOrdering ordering
    ) throws IOException {
        Objects.requireNonNull(ordering, "ordering");
        ensureOpen();

        reserveBreaker("buildArrowManaged");

        MVCompiledDefinition.OrderingFFIMetadata ffi = MVCompiledDefinition.OrderingFFIMetadata.from(ordering);
        long contextId = MVNativeBridge.allocateCancellationContext();
        activeContextId.set(contextId);

        try {
            long result = MVNativeBridge.buildArrowManaged(
                runtimePtr,
                parquetInput,
                tableName,
                filteredSql,
                arrayAddr,
                schemaAddr,
                ffi.fieldIndices(),
                ffi.directionTokens(),
                ffi.nullPlacementTokens(),
                contextId,
                spillBudgetBytes,
                spillFileCountLimit
            );
            MVBuildMetrics.INSTANCE.recordBuildCompleted();
            return result;
        } catch (Exception e) {
            MVBuildMetrics.INSTANCE.recordBuildFailed();
            if (e instanceof IOException) throw (IOException) e;
            throw new IOException("mv_pull arrow managed build failed", e);
        } finally {
            activeContextId.set(0);
            MVNativeBridge.releaseCancellationContext(contextId);
            releaseBreaker("buildArrowManaged");
        }
    }

    /**
     * Cancel any active build on this runtime. Called from a different thread
     * (e.g., task cancellation, shard close).
     */
    public void cancel() {
        long id = activeContextId.get();
        if (id != 0) {
            MVNativeBridge.cancelBuild(id);
        }
    }

    /**
     * Returns the shared DataFusionRuntime pointer for callers that need
     * to pass it to other native operations on the same runtime.
     */
    public long runtimePtr() {
        ensureOpen();
        return runtimePtr;
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("MVBuildRuntime is closed");
        }
    }

    /**
     * Reserve memory against the OpenSearch circuit breaker.
     * Throws CircuitBreakingException (wrapped in IOException) if the breaker trips.
     */
    private void reserveBreaker(String label) throws IOException {
        if (circuitBreaker != null && buildMemoryEstimate > 0) {
            try {
                circuitBreaker.addEstimateBytesAndMaybeBreak(buildMemoryEstimate, label);
                MVBuildMetrics.INSTANCE.recordBreakerReservation(buildMemoryEstimate);
                logger.trace("mv_pull breaker reserved {} bytes for {}", buildMemoryEstimate, label);
            } catch (CircuitBreakingException cbe) {
                MVBuildMetrics.INSTANCE.recordBreakerTrip();
                throw new IOException(
                    "mv_pull circuit breaker tripped during "
                        + label
                        + ": limit="
                        + cbe.getBytesWanted()
                        + " used="
                        + cbe.getByteLimit()
                        + " delta="
                        + buildMemoryEstimate,
                    cbe
                );
            }
        }
    }

    /**
     * Release the circuit breaker reservation after the build completes.
     */
    private void releaseBreaker(String label) {
        if (circuitBreaker != null && buildMemoryEstimate > 0) {
            circuitBreaker.addWithoutBreaking(-buildMemoryEstimate);
            MVBuildMetrics.INSTANCE.recordBreakerRelease(buildMemoryEstimate);
            logger.trace("mv_pull breaker released {} bytes for {}", buildMemoryEstimate, label);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            cancel(); // cancel any in-flight build
        }
    }

    /**
     * @deprecated Stage 4: Use {@link MVCompiledDefinition.OrderingFFIMetadata}
     *             directly. This inner record is retained only for source
     *             compatibility with any in-flight patches.
     */
    @Deprecated
    record MVOrderingFFI(int[] fieldIndices, int[] directionTokens, int[] nullPlacementTokens) {

        static MVOrderingFFI from(MVGroupByOrdering ordering) {
            MVCompiledDefinition.OrderingFFIMetadata meta = MVCompiledDefinition.OrderingFFIMetadata.from(ordering);
            return new MVOrderingFFI(meta.fieldIndices(), meta.directionTokens(), meta.nullPlacementTokens());
        }
    }

    /**
     * Stage 3: Result of a streaming artifact build, including validation metadata
     * and instrumentation from the native {@code MvBuildResult} struct.
     *
     * @param rowCount           number of state rows in the artifact
     * @param schemaHash         u64 FNV hash of the Arrow schema (from native)
     * @param definitionHash     u64 FNV hash of the ordering contract (from native)
     * @param orderingHash       u64 FNV hash of the ordering identity (from native)
     * @param spillBytes         total bytes spilled to disk during build
     * @param spillFileCount     number of spill files created
     * @param outputBatchCount   number of Arrow IPC batches written
     * @param peakRssBytes       peak resident set size during build
     * @param buildDurationUs    wall-clock build duration in microseconds
     * @param statusCode         native status code (0=OK, 1=cancelled, etc.)
     */
    public record ArtifactResult(
        long rowCount,
        long schemaHash,
        long definitionHash,
        long orderingHash,
        long spillBytes,
        int spillFileCount,
        int outputBatchCount,
        long peakRssBytes,
        long buildDurationUs,
        int statusCode
    ) {
        public ArtifactResult {
            if (statusCode == MvBuildResultLayout.STATUS_OK && rowCount <= 0) {
                throw new IllegalArgumentException("rowCount must be positive for OK status, got " + rowCount);
            }
        }

        /** Backwards-compatible constructor for tests that only need rowCount + string hashes. */
        public ArtifactResult(long rowCount, String schemaHashStr, String definitionHashStr) {
            this(rowCount, 0L, 0L, 0L, 0L, 0, 0, 0L, 0L, MvBuildResultLayout.STATUS_OK);
            if (rowCount <= 0) {
                throw new IllegalArgumentException("rowCount must be positive, got " + rowCount);
            }
            java.util.Objects.requireNonNull(schemaHashStr, "schemaHash");
            java.util.Objects.requireNonNull(definitionHashStr, "definitionHash");
        }

        /** Returns true when the native build completed successfully. */
        public boolean isOk() {
            return statusCode == MvBuildResultLayout.STATUS_OK;
        }

        /** Returns true when the build was cancelled. */
        public boolean isCancelled() {
            return statusCode == MvBuildResultLayout.STATUS_CANCELLED;
        }
    }
}
