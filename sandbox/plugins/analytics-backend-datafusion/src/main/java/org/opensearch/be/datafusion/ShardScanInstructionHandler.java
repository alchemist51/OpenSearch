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
 * <p>For derived materialized-view targets — indices that carry the derived
 * data-format setting {@code index.derived.data_format=materialized_view}
 * stamped by {@code MVViewCreation} at view creation — the handler instead
 * opens an MV-only session: it resolves the catalog-published state files
 * (artifact format {@code mv_state}, published by {@code MVTargetHydrator}) and
 * the ordered state fields ({@code index.mv.state_fields}), and calls
 * {@link NativeBridge#createMVOnlySessionContext} so the DataFusion MV-only
 * state scan folds over the state files. When the setting is absent it falls
 * through to the normal raw scan path unchanged.
 */
public class ShardScanInstructionHandler implements FragmentInstructionHandler<ShardScanInstructionNode> {

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(
        ShardScanInstructionHandler.class
    );

    /**
     * Index setting stamped by {@code MVViewCreation.commonTargetSettings()} on a
     * derived MV target ({@code MVConstants.DERIVED_DATA_FORMAT_SETTING}). Its
     * presence with value {@link #MV_DATA_FORMAT_CATEGORY} routes the shard scan
     * to the MV-only fold session. Read directly from the raw index settings —
     * mv-engine owns the declaration, so no additional registration is required.
     */
    static final String DERIVED_DATA_FORMAT_SETTING = "index.derived.data_format";

    /** Canonical derived data-format category for materialized views ({@code MVConstants.DATA_FORMAT_NAME}). */
    static final String MV_DATA_FORMAT_CATEGORY = "materialized_view";

    /** Ordered state-column names on the target ({@code MVConstants.STATE_FIELDS_SETTING}). */
    static final String STATE_FIELDS_SETTING = "index.mv.state_fields";

    /**
     * Catalog artifact format name under which {@code MVTargetHydrator} publishes
     * hydrated state files (see {@code MVTargetHydrator.MV_STATE_FORMAT_NAME}).
     */
    static final String MV_STATE_ARTIFACT_NAME = "mv_state";

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

        // Dispatch on the derived data-format setting stamped at view creation.
        // An index declaring index.derived.data_format=materialized_view serves
        // PARTIAL AGGREGATE STATE and must take the MV-only fold session, never
        // the raw row path. Absent setting => normal path (fall through).
        boolean mvServing = isDerivedMVTarget(context.getIndexSettings().getSettings());

        long runtimePtr = dataFusionService.getNativeRuntime().get();
        long contextId = context.getTask() != null ? context.getTask().getId() : 0L;

        WireConfigSnapshot snapshot = plugin.getDatafusionSettings().getSnapshot();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment segment = arena.allocate(WireConfigSnapshot.BYTE_SIZE);
            snapshot.writeTo(segment);

            if (mvServing) {
                // MV-only path: resolve state files from the catalog snapshot and the
                // ordered state fields from settings, then open the MV-only session.
                // The coordinator captured the logical table name from the plan's
                // table-scan leaf; fall back to the concrete index name when absent.
                String tableName = node.getLogicalTableName() != null ? node.getLogicalTableName() : context.getTableName();
                java.util.List<String> stateFilePaths = resolveMVStateFiles(context);
                java.util.List<String> stateFields = resolveMVStateFields(context);
                SessionContextHandle sessionCtxHandle = NativeBridge.createMVOnlySessionContext(
                    runtimePtr,
                    tableName,
                    contextId,
                    context.hasPartialAggregate(),
                    segment.address(),
                    context.getFragmentBytes(),
                    stateFilePaths,
                    stateFields
                );
                LOGGER.info(
                    "mv read: created MV-only session with {} state files for [{}]",
                    stateFilePaths.size(),
                    context.getIndexSettings().getIndex().getName()
                );
                return new DataFusionSessionState(sessionCtxHandle);
            }

            // Normal path: acquire a DatafusionReader for parquet/composite formats.
            DatafusionReader dfReader = null;
            for (String formatName : plugin.getSupportedFormats()) {
                dfReader = context.getReader().getReader(registry.format(formatName), DatafusionReader.class);
                if (dfReader != null) break;
            }
            if (dfReader == null) {
                throw new IllegalStateException("No DatafusionReader available in the acquired reader");
            }

            long readerPtr = dfReader.getReaderHandle().getPointer();
            // The coordinator captured the logical table name (alias / index pattern / index the query
            // referenced) from the plan's table-scan leaf. Register the shard's table under it so the
            // Substrait plan's NamedTable binds. Fall back to the concrete shard index name when absent.
            String tableName = node.getLogicalTableName() != null ? node.getLogicalTableName() : context.getTableName();

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
            return new DataFusionSessionState(sessionCtxHandle);
        }
    }

    /**
     * Returns true when the index settings declare the derived materialized-view
     * data-format category ({@code index.derived.data_format=materialized_view}),
     * which routes the shard scan to the MV-only fold session.
     */
    static boolean isDerivedMVTarget(org.opensearch.common.settings.Settings settings) {
        return MV_DATA_FORMAT_CATEGORY.equals(settings.get(DERIVED_DATA_FORMAT_SETTING));
    }

    /**
     * Resolves the ordered MV state-column names from {@code index.mv.state_fields}.
     * The list is the positional contract consumed by the Rust MV expr adapter.
     */
    static java.util.List<String> resolveMVStateFields(ShardScanExecutionContext context) {
        java.util.List<String> stateFields = context.getIndexSettings().getSettings().getAsList(STATE_FIELDS_SETTING);
        if (stateFields == null || stateFields.isEmpty()) {
            throw new IllegalStateException(
                "derived materialized-view target requires ordered "
                    + STATE_FIELDS_SETTING
                    + " metadata (index="
                    + context.getIndexSettings().getIndex().getName()
                    + ")"
            );
        }
        return java.util.List.copyOf(stateFields);
    }

    /**
     * Resolves the absolute paths of the catalog-published MV state files.
     * The push-based hydrator publishes them under the {@code mv_state} artifact
     * format; the pull-based builder ({@code MVDerivedArtifactBuilder}) publishes
     * them as stock {@code parquet} generations of the derived target
     * ({@code MVConstants.STATE_ARTIFACT_FORMAT}). A derived MV target holds no
     * raw rows, so every parquet generation in its catalog is MV state; both
     * formats are therefore admitted, {@code mv_state} first.
     */
    static final String PULL_STATE_ARTIFACT_FORMAT = "parquet";

    static java.util.List<String> resolveMVStateFiles(ShardScanExecutionContext context) {
        org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot = context.getReader().catalogSnapshot();
        java.util.Collection<org.opensearch.index.engine.exec.WriterFileSet> stateSets = snapshot.getSearchableFiles(
            MV_STATE_ARTIFACT_NAME
        );
        if (stateSets == null || stateSets.isEmpty()) {
            stateSets = snapshot.getSearchableFiles(PULL_STATE_ARTIFACT_FORMAT);
        }
        if (stateSets == null || stateSets.isEmpty()) {
            throw new IllegalStateException(
                "derived materialized-view target has no ["
                    + MV_STATE_ARTIFACT_NAME
                    + "] or ["
                    + PULL_STATE_ARTIFACT_FORMAT
                    + "] state files in the catalog snapshot (index="
                    + context.getIndexSettings().getIndex().getName()
                    + ")"
            );
        }
        return stateSets.stream().flatMap(fs -> fs.files().stream().map(f -> fs.directory() + "/" + f)).sorted().toList();
    }
}
