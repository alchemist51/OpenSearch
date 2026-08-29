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
 *
 * <p>For MV target indices with {@code index.mv.serve_state=true}, after the session
 * is created the handler attaches the pre-computed Arrow state files which REPLACE
 * the parquet scan with pre-aggregated Partial output.
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

        // Check MV serve_state FIRST: if set, always use the MV-only path regardless
        // of whether a DatafusionReader can be acquired (the mv_state format may register
        // a reader, but it's not usable for creating a normal session context).
        boolean mvServing = isMVServing(context);

        DatafusionReader dfReader = null;
        if (!mvServing) {
            // Normal path: try to acquire a reader for parquet/composite formats.
            for (String formatName : plugin.getSupportedFormats()) {
                var format = registry.format(formatName);
                if (format != null) {
                    dfReader = context.getReader().getReader(format, DatafusionReader.class);
                    if (dfReader != null) break;
                }
            }
            if (dfReader == null) {
                throw new IllegalStateException("No DatafusionReader available in the acquired reader");
            }
        }

        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;
        String tableName = context.getTableName();

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);
            SessionContextHandle sessionCtxHandle;

            if (dfReader != null) {
                // Normal path: we have a parquet/lucene reader.
                long readerPtr = dfReader.getReaderHandle().getPointer();
                if (node.requestsRowIds()) {
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
            } else {
                // MV-only path: resolve state file paths from catalog, then create session
                // with Arrow data registered directly — no separate sessionAttachMV needed.
                java.util.List<String> stateFilePaths = resolveMVStateFiles(context);
                java.util.List<String> stateFields = resolveMVStateFields(context);
                sessionCtxHandle = NativeBridge.createMVOnlySessionContext(
                    runtimePtr,
                    tableName,
                    contextId,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes(),
                    stateFilePaths,
                    stateFields
                );
                org.apache.logging.log4j.LogManager.getLogger(ShardScanInstructionHandler.class)
                    .info(
                        "mv read: created MV session with {} state files for [{}]",
                        stateFilePaths.size(),
                        context.getIndexSettings().getIndex().getName()
                    );
            }

            return new DataFusionSessionState(sessionCtxHandle);
        }
    }

    private static boolean isMVServing(ShardScanExecutionContext context) {
        org.opensearch.index.IndexSettings indexSettings = context.getIndexSettings();
        return indexSettings != null && indexSettings.getSettings().getAsBoolean("index.mv.serve_state", false);
    }

    private static java.util.List<String> resolveMVStateFields(ShardScanExecutionContext context) {
        java.util.List<String> stateFields = context.getIndexSettings().getSettings().getAsList("index.mv.state_fields");
        if (stateFields == null || stateFields.isEmpty()) {
            throw new IllegalStateException(
                "index.mv.serve_state requires ordered index.mv.state_fields metadata (index="
                    + context.getIndexSettings().getIndex().getName()
                    + ")"
            );
        }
        return java.util.List.copyOf(stateFields);
    }

    private static java.util.List<String> resolveMVStateFiles(ShardScanExecutionContext context) {
        org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot = context.getReader().catalogSnapshot();
        java.util.Collection<org.opensearch.index.engine.exec.WriterFileSet> stateSets = snapshot.getSearchableFiles("mv_state");
        if (stateSets == null || stateSets.isEmpty()) {
            throw new IllegalStateException(
                "index.mv.serve_state is set but the catalog snapshot has no mv_state files (index="
                    + context.getIndexSettings().getIndex().getName()
                    + ")"
            );
        }
        return stateSets.stream().flatMap(fs -> fs.files().stream().map(f -> fs.directory() + "/" + f)).sorted().toList();
    }

}
