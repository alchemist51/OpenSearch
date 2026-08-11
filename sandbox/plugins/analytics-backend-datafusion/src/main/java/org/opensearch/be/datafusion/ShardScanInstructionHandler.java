/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.CommonExecutionContext;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.be.datafusion.nativelib.SessionContextHandle;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handles ShardScan instruction: creates a SessionContext via FFM and registers
 * the default ListingTable provider for parquet scans. When the instruction node
 * carries an MV rewrite binding, additionally attaches the shard-local coverage
 * split (state file paths + covered raw file names) to the native session.
 */
public class ShardScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private static final Logger logger = LogManager.getLogger(ShardScanInstructionHandler.class);

    /**
     * Composite-engine format name of the derived MV state format. A string constant
     * (not a class reference) because the format is owned by the mv-data-format plugin,
     * which depends on this plugin — the reverse dependency would be circular.
     */
    static final String MV_FORMAT_NAME = "materialized_view";

    /** Composite-engine format name of the primary columnar format this backend scans. */
    static final String PARQUET_FORMAT_NAME = "parquet";

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
                if (node.hasMVBinding()) {
                    attachMVBinding(node, context, sessionCtxHandle);
                }
            }
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }

    /**
     * Computes the shard-local MV coverage split from the acquired reader's catalog
     * snapshot and attaches it to the native session. Coverage is a generation-set
     * intersection: a raw parquet segment is covered iff a {@code materialized_view}
     * file set with the same writer generation exists in the SAME snapshot (so the
     * split is atomic with the snapshot). Zero coverage → no attach → today's plan.
     *
     * <p>The binding is an option, not an obligation: the native side re-validates
     * the state-file schema at prepare time and falls back to the raw-only plan on
     * any mismatch (never wrong, only slower).
     */
    private static void attachMVBinding(ShardScanInstructionNode node, ShardScanExecutionContext context, SessionContextHandle handle) {
        try {
            CatalogSnapshot snapshot = context.getReader().catalogSnapshot();
            Collection<WriterFileSet> mvSets = snapshot.getSearchableFiles(MV_FORMAT_NAME);
            if (mvSets == null || mvSets.isEmpty()) {
                return;
            }
            Set<Long> coveredGenerations = mvSets.stream().map(WriterFileSet::writerGeneration).collect(Collectors.toSet());
            List<String> mvFilePaths = mvSets.stream().flatMap(s -> s.files().stream().map(f -> s.directory() + "/" + f)).sorted().toList();
            // Raw parquet file NAMES of covered generations — excluded from the raw scan
            // when the MV branch is taken. Name (not path) matching: the native side
            // compares against the last path segment of its object-store locations.
            List<String> coveredRawFileNames = snapshot.getSearchableFiles(PARQUET_FORMAT_NAME)
                .stream()
                .filter(s -> coveredGenerations.contains(s.writerGeneration()))
                .flatMap(s -> s.files().stream())
                .sorted()
                .toList();
            NativeBridge.sessionAttachMV(handle.getPointer(), mvFilePaths, coveredRawFileNames);
            logger.info(
                "mv-binding [{}]: attached {} state files, {} covered raw files",
                node.mvId(),
                mvFilePaths.size(),
                coveredRawFileNames.size()
            );
        } catch (Exception e) {
            // The binding is best-effort: any failure here must leave the session in
            // its default (raw-only) state rather than failing the query.
            logger.warn("mv-binding [{}]: attach failed, falling back to raw scan", node.mvId(), e);
        }
    }
}
