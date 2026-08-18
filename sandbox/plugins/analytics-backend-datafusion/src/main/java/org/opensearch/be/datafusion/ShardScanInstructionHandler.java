/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

/**
 * Handles ShardScan instruction: creates a SessionContext via FFM and registers
 * the default ListingTable provider for parquet scans.
 */
public class ShardScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private final DataFusionPlugin plugin;

    ShardScanInstructionHandler(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public BackendExecutionContext apply(
        ShardScanInstructionNode node,
        CommonExecutionContext commonContext,
        BackendExecutionContext backendContext
    ) {
        ShardScanExecutionContext context = (ShardScanExecutionContext) commonContext;
        DataFusionService dataFusionService = plugin.getDataFusionService();
        DataFormatRegistry registry = plugin.getDataFormatRegistry();

        DatafusionReader dfReader = null;
        for (String formatName : plugin.getSupportedFormats()) {
            dfReader = context.getReader().getReader(registry.format(formatName), DatafusionReader.class);
            if (dfReader != null) break;
        }
        if (dfReader == null) {
            throw new IllegalStateException("No DatafusionReader available in the acquired reader");
        }

        long readerPtr = dfReader.getReaderHandle().getPointer();
        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        String tableName = context.getTableName();

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            SessionContextHandle sessionCtxHandle;
            if (node.requestsRowIds()) {
                // QTF query phase — narrowed scan emits __row_id__. Use the indexed session
                // context so the IndexedTableProvider injects shard-global row ids during scan.
                // No delegated predicates here (delegation goes through ShardScanWithDelegationHandler),
                // so treeShape=NO_DELEGATION and delegatedPredicateCount=0.
                sessionCtxHandle = NativeBridge.createSessionContextForIndexedExecution(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    FilterTreeShape.NO_DELEGATION.ordinal(),
                    0,
                    true,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            } else {
                // Plan bytes let Rust widen the schema for multi-index queries (null-fill missing columns).
                sessionCtxHandle = NativeBridge.createSessionContext(
                    readerPtr,
                    runtimePtr,
                    tableName,
                    contextId,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes()
                );
            }
            attachMVStateIfServing(context, sessionCtxHandle);
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }

    /**
     * Validation-scoped MV read (crude by design, decision pending prod shape):
     * when the scanned index carries {@code index.mv.serve_state=true} (a
     * dynamic setting on the MV TARGET index), attach its own catalog
     * snapshot's {@code mv_state} Arrow files to the native session in STRICT
     * mode. The prepared plan's Partial is then REPLACED by the state-file
     * scan — the files ARE Partial output (zero-translation contract:
     * the definition is the query, state columns positionally match the
     * query's partial schema) — and the coordinator's Final merges and
     * evaluates natively (avg = merge counts+sums, divide once at the end).
     * STRICT: any misalignment (schema, types, plan shape) is a hard error —
     * never a silent fallback, never a wrong answer.
     */
    private static void attachMVStateIfServing(ShardScanExecutionContext context, SessionContextHandle handle) {
        org.opensearch.index.IndexSettings indexSettings = context.getIndexSettings();
        if (indexSettings == null || indexSettings.getSettings().getAsBoolean("index.mv.serve_state", false) == false) {
            return;
        }
        org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot = context.getReader().catalogSnapshot();
        java.util.Collection<org.opensearch.index.engine.exec.WriterFileSet> stateSets = snapshot.getSearchableFiles("mv_state");
        if (stateSets == null || stateSets.isEmpty()) {
            throw new IllegalStateException(
                "index.mv.serve_state is set but the catalog snapshot has no mv_state files (index="
                    + indexSettings.getIndex().getName()
                    + ")"
            );
        }
        java.util.List<String> stateFilePaths = stateSets.stream()
            .flatMap(fs -> fs.files().stream().map(f -> fs.directory() + "/" + f))
            .sorted()
            .toList();
        // Strict always: this read mode exists to VALIDATE the MV mechanism —
        // a fallback to raw scans would silently invalidate the experiment.
        org.apache.logging.log4j.LogManager.getLogger(ShardScanInstructionHandler.class)
            .info(
                "mv read: serving {} state files as Partial output (strict) for [{}]",
                stateFilePaths.size(),
                indexSettings.getIndex().getName()
            );
        NativeBridge.sessionAttachMV(handle.getPointer(), stateFilePaths, java.util.List.of(), true);
    }
}
