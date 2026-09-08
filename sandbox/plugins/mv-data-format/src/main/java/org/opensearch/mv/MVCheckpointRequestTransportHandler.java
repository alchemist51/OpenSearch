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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Source-side handler for {@link MVCheckpointRequestAction}. Performs the full
 * scoped checkpoint construction that was previously the publisher's role:
 *
 * <ol>
 *   <li>Resolves the source shard via {@link IndicesService} in request scope
 *       (no cached IndexShard refs)</li>
 *   <li>Reads the catalog snapshot</li>
 *   <li>Computes advertMax = max(fileset.maxSeqNo)</li>
 *   <li>Returns nothing-new if advertMax &le; request watermark</li>
 *   <li>Filters files to (requestWatermark, advertMax]</li>
 *   <li>Scopes noops from {@link MVNoopTracker} to the same range</li>
 *   <li>Triggers noop eviction below the requesting target's watermark
 *       (per-source-shard min-requested-watermark tracking)</li>
 *   <li>Builds and returns {@link MVReplicationCheckpoint}</li>
 * </ol>
 *
 * <p>Runs on the GENERIC thread pool executor.</p>
 */
public final class MVCheckpointRequestTransportHandler extends org.opensearch.action.support.single.shard.TransportSingleShardAction<
    MVCheckpointRequestAction.Request,
    MVCheckpointRequestAction.Response> {

    private static final Logger logger = LogManager.getLogger(MVCheckpointRequestTransportHandler.class);

    private final IndicesService indicesService;
    private final MVNoopTracker noopTracker;

    /**
     * Per-source-shard minimum watermark seen across all requesting targets.
     * Used for noop eviction — we evict below this because all known targets
     * have progressed past it. Updated on each request.
     */
    private final java.util.concurrent.ConcurrentHashMap<ShardId, Long> minRequestedWatermarks =
        new java.util.concurrent.ConcurrentHashMap<>();

    @Inject
    public MVCheckpointRequestTransportHandler(
        org.opensearch.threadpool.ThreadPool threadPool,
        org.opensearch.cluster.service.ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService,
        MVNoopTracker noopTracker
    ) {
        super(
            MVCheckpointRequestAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MVCheckpointRequestAction.Request::new,
            ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
        this.noopTracker = noopTracker;
    }

    /** Route to the node holding the SOURCE shard's active primary (relocation-safe). */
    @Override
    protected org.opensearch.cluster.routing.ShardIterator shards(
        org.opensearch.cluster.ClusterState state,
        InternalRequest request
    ) {
        return state.routingTable()
            .index(request.request().sourceIndex())
            .shard(request.request().sourceShard())
            .primaryShardIt();
    }

    @Override
    protected boolean resolveIndex(MVCheckpointRequestAction.Request request) {
        return true;
    }

    @Override
    protected org.opensearch.core.common.io.stream.Writeable.Reader<MVCheckpointRequestAction.Response> getResponseReader() {
        return MVCheckpointRequestAction.Response::new;
    }

    @Override
    protected MVCheckpointRequestAction.Response shardOperation(MVCheckpointRequestAction.Request request, ShardId requestShardId)
        throws java.io.IOException {
        // Transport routing guarantees this node holds the source primary;
        // resolve locally in request scope (never cache IndexShard refs).
        IndexShard shard = indicesService.indexServiceSafe(requestShardId.getIndex()).getShard(requestShardId.id());
        if (!shard.routingEntry().primary() || !shard.routingEntry().active()) {
            // Relocation race: surface as retryable failure, never a silent nothing-new.
            throw new org.opensearch.action.NoShardAvailableActionException(
                requestShardId,
                "source primary not active on routed node"
            );
        }
        {

            ShardId shardId = shard.shardId();
            long requestWatermark = request.targetWatermark();

            // ── Read catalog snapshot, filtered to REMOTE-VISIBLE filesets ──
            // Defect #30: the local catalog includes parquet generations whose
            // remote-store upload has not completed/become visible yet. Core
            // SegRep publishes replication checkpoints from the upload-completed
            // path — the remote metadata is the single source of truth for
            // cross-node visibility. Mirror that invariant here: advertise a
            // fileset (and count its seqNo range toward advertMax) only when
            // EVERY file of the fileset is present in the shard's uploaded-to-
            // remote map. Filtering is per-FILESET, not per-file — a partially
            // uploaded generation must not contribute its maxSeqNo, or the
            // target stages a range it cannot cover (the observed 2-4 doc
            // coverage-mismatch livelock under S3 upload lag). With an FS
            // remote store uploads are effectively instantaneous and this
            // filter passes everything, matching prior single-node behaviour.
            Map<String, MVFileMetadata> allFileMetadata = new LinkedHashMap<>();
            long infosVersion;
            long catalogAdvertMax = -1L;
            long primaryTerm;
            boolean anyUnknownRange = false;
            long skippedFilesets = 0L;

            final java.util.Set<String> remoteVisible;
            if (shard.indexSettings().isRemoteStoreEnabled()) {
                remoteVisible = java.util.Set.copyOf(shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore().keySet());
            } else {
                // Non-remote source (local-only dev): nothing to gate on —
                // preserve pre-#30 behaviour exactly.
                remoteVisible = null;
            }

            try (GatedCloseable<CatalogSnapshot> ref = shard.getCatalogSnapshot()) {
                CatalogSnapshot catalog = ref.get();
                infosVersion = catalog.getVersion();
                primaryTerm = shard.getOperationPrimaryTerm();

                for (Segment seg : catalog.getSegments()) {
                    for (Map.Entry<String, WriterFileSet> fsEntry : seg.dfGroupedSearchableFiles().entrySet()) {
                        if (!"parquet".equals(fsEntry.getKey())) continue;
                        WriterFileSet wfs = fsEntry.getValue();

                        // Visibility gate: every file of this fileset must be
                        // uploaded before any of it is advertised.
                        if (!filesetRemoteVisible(wfs, fsEntry.getKey(), remoteVisible)) {
                            skippedFilesets++;
                            continue;
                        }

                        Path dir = Path.of(wfs.directory());
                        for (String fileName : wfs.files()) {
                            long size = -1L;
                            try {
                                Path filePath = dir.resolve(fileName);
                                if (Files.exists(filePath)) {
                                    size = Files.size(filePath);
                                }
                            } catch (Exception ignored) {}

                            if (wfs.maxSeqNo() < 0) {
                                anyUnknownRange = true;
                            }

                            String remoteKey = FileMetadata.serialize(fsEntry.getKey(), fileName);
                            allFileMetadata.put(remoteKey, new MVFileMetadata(
                                size, wfs.minSeqNo(), wfs.maxSeqNo(), MVFileMetadata.CRC32_UNKNOWN
                            ));

                            if (wfs.maxSeqNo() >= 0 && wfs.maxSeqNo() > catalogAdvertMax) {
                                catalogAdvertMax = wfs.maxSeqNo();
                            }
                        }
                    }
                }
            }
            if (skippedFilesets > 0) {
                logger.info(
                    "CHECKPOINT_VISIBILITY_SKIP source=[{}][{}] filesets_not_yet_remote_visible={} advertMax={}",
                    request.sourceIndex(),
                    request.sourceShard(),
                    skippedFilesets,
                    catalogAdvertMax
                );
            }

            // ── Unknown range = BUG: log WARN and return unavailable ─────
            if (anyUnknownRange) {
                logger.warn(
                    "CHECKPOINT_REPLY: source shard [{}][{}] has fileset(s) with unknown seq range — skipping",
                    request.sourceIndex(),
                    request.sourceShard()
                );
                return MVCheckpointRequestAction.Response.unavailable();
            }

            // ── Nothing-new: advertMax <= requestWatermark ───────────────
            if (catalogAdvertMax <= 0 || catalogAdvertMax <= requestWatermark) {
                logger.debug(
                    "CHECKPOINT_NOTHING_NEW source=[{}][{}] target=[{}][{}] advertMax={} watermark={}",
                    request.sourceIndex(),
                    request.sourceShard(),
                    request.targetIndex(),
                    request.targetShard(),
                    catalogAdvertMax,
                    requestWatermark
                );
                return MVCheckpointRequestAction.Response.unavailable();
            }

            long advertMax = catalogAdvertMax;

            // ── Filter files to (requestWatermark, advertMax] ────────────
            Map<String, MVFileMetadata> scopedFiles = new LinkedHashMap<>();
            for (Map.Entry<String, MVFileMetadata> entry : allFileMetadata.entrySet()) {
                MVFileMetadata meta = entry.getValue();
                if (includeFile(meta.minSeqNo(), meta.maxSeqNo(), requestWatermark, advertMax)) {
                    scopedFiles.put(entry.getKey(), meta);
                }
            }

            // ── Scope noops to (requestWatermark, advertMax] ─────────────
            long[] scopedNoops;
            if (noopTracker != null) {
                scopedNoops = noopTracker.getNoopsInRange(shardId, requestWatermark, advertMax);
            } else {
                scopedNoops = new long[0];
            }

            // ── Evict noops below requesting target's watermark ──────────
            // Track the minimum watermark seen across all requesters for this
            // source shard. Evict below it — safe because all known targets
            // have progressed past that point.
            if (noopTracker != null && requestWatermark >= 0) {
                minRequestedWatermarks.merge(shardId, requestWatermark, Math::min);
                long minWm = minRequestedWatermarks.get(shardId);
                noopTracker.evictBelow(shardId, minWm);
            }

            MVReplicationCheckpoint checkpoint = new MVReplicationCheckpoint(
                request.sourceIndex(),
                request.sourceShard(),
                primaryTerm,
                advertMax,
                infosVersion,
                scopedFiles,
                System.currentTimeMillis(),
                scopedNoops
            );

            logger.info(
                "CHECKPOINT_REPLY source=[{}][{}] target=[{}][{}] files={} noops={} advertMax={} watermark={}",
                request.sourceIndex(),
                request.sourceShard(),
                request.targetIndex(),
                request.targetShard(),
                scopedFiles.size(),
                scopedNoops.length,
                advertMax,
                requestWatermark
            );

            return new MVCheckpointRequestAction.Response(checkpoint);
        }
    }

    // ── File filtering ──────────────────────────────────────────────────

    /**
     * Determines whether a file should be included in the checkpoint for a target.
     * A file is included if:
     * <ul>
     *   <li>Its maxSeqNo is unknown (-1) — legacy/fail-open, always include</li>
     *   <li>Its [minSeqNo, maxSeqNo] range intersects (watermark, sourceMaxSeqNo]</li>
     * </ul>
     * A file is excluded only when its entire seq range is at or below the watermark.
     */
    static boolean includeFile(long fileMinSeqNo, long fileMaxSeqNo, long targetWatermark, long sourceMaxSeqNo) {
        if (fileMaxSeqNo == -1L) {
            return true;
        }
        if (fileMaxSeqNo <= targetWatermark) {
            return false;
        }
        return true;
    }

    /**
     * Defect #30 visibility gate: a fileset may be advertised only when EVERY
     * one of its files is present in the source shard's uploaded-to-remote map
     * ({@code remoteVisible}, keyed by {@link FileMetadata#serialize}). A
     * partially uploaded generation must not contribute files or its maxSeqNo —
     * the target would stage a range it cannot cover. {@code remoteVisible ==
     * null} means the source has no remote store (local-only dev): fail open,
     * preserving pre-#30 behaviour.
     */
    static boolean filesetRemoteVisible(WriterFileSet wfs, String format, java.util.Set<String> remoteVisible) {
        if (remoteVisible == null) {
            return true;
        }
        for (String fileName : wfs.files()) {
            if (!remoteVisible.contains(FileMetadata.serialize(format, fileName))) {
                return false;
            }
        }
        return true;
    }

    // ── Test accessors ───────────────────────────────────────────────────

    java.util.concurrent.ConcurrentHashMap<ShardId, Long> minRequestedWatermarks() {
        return minRequestedWatermarks;
    }
}
