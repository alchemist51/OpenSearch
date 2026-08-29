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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Applies Arrow MV state batches on the local derived-index primary through
 * its replication-only write capability. This is logical derived-state
 * replication, not user document indexing: callers cannot reach this path
 * through bulk/index APIs, and the target has no active translog.
 *
 * <p>The acknowledgement means the batch is applied and searchable. Target
 * durability advances later, after the source commit asynchronously signals
 * a checkpoint covering this exact claim; a crash before that target commit
 * is repaired from authoritative source parquet.
 *
 * <p>Runs on the WRITE pool. Locality is a HARD PRECONDITION: the ordinal-
 * paired target primary must be on this node (the colocation decider's job) —
 * a split pair fails the ship, which fails the flush (ship-before-commit
 * backpressure) until reactive following restores the pair. There is no
 * remote-forward path; NodeClient dispatch means nothing is ever serialized.
 *
 * <p>POC scope: primary-only apply (no replication chain; POC targets run
 * with zero replicas). Production replaces this with a
 * TransportWriteAction-style replicated apply.
 */
public final class MVShipStateTransportHandler extends HandledTransportAction<MVShipStateAction.Request, MVShipStateAction.Response> {

    private static final Logger logger = LogManager.getLogger(MVShipStateTransportHandler.class);

    private final IndicesService indicesService;
    private final org.opensearch.cluster.service.ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public MVShipStateTransportHandler(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        super(MVShipStateAction.NAME, transportService, actionFilters, MVShipStateAction.Request::new, ThreadPool.Names.WRITE);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    protected void doExecute(Task task, MVShipStateAction.Request request, ActionListener<MVShipStateAction.Response> listener) {
        // Any exit that does not enter applyLocally must release this
        // handler's reference on the shared batch — applyLocally releases in
        // its own finally once entered.
        boolean handedOff = false;
        try {
            // I6 fingerprint gate: reject ships from a different definition
            // than this target serves (definition changes must not fold
            // foreign state into certified answers).
            if (request.definition() != null) {
                String targetDefinition = clusterService.state()
                    .metadata()
                    .index(request.targetIndex())
                    .getSettings()
                    .get(MVConstants.DEFINITION_SETTING);
                if (targetDefinition != null && targetDefinition.equals(request.definition()) == false) {
                    listener.onFailure(
                        new IllegalStateException(
                            "mv ship: definition fingerprint mismatch — source ships ["
                                + request.definition()
                                + "] but target ["
                                + request.targetIndex()
                                + "] serves ["
                                + targetDefinition
                                + "]"
                        )
                    );
                    return;
                }
            }
            org.opensearch.cluster.routing.ShardRouting primary = clusterService.state()
                .routingTable()
                .index(request.targetIndex())
                .shard(request.targetShard())
                .primaryShard();
            if (primary == null || primary.active() == false || primary.currentNodeId() == null) {
                listener.onFailure(
                    new IllegalStateException(
                        "mv ship: target primary [" + request.targetIndex() + "][" + request.targetShard() + "] is not active"
                    )
                );
                return;
            }
            String localNodeId = clusterService.localNode().getId();
            if (primary.currentNodeId().equals(localNodeId) == false) {
                // HARD RULE: the ordinal-paired target primary must be local at
                // ship time. No remote fallback — a split pair fails the flush
                // (ship-before-commit backpressure), and the colocation
                // decider's reactive following restores the pair; the retried
                // flush then succeeds. One path, one failure mode.
                listener.onFailure(
                    new IllegalStateException(
                        "mv ship: target primary ["
                            + request.targetIndex()
                            + "]["
                            + request.targetShard()
                            + "] is on node ["
                            + primary.currentNodeId()
                            + "], not local ["
                            + localNodeId
                            + "] — colocation is a ship precondition"
                    )
                );
                return;
            }
            logger.info("mv ship-apply path=local target=[{}][{}]", request.targetIndex(), request.targetShard());
            handedOff = true;
            applyLocally(request, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            if (handedOff == false) {
                request.stateBatch().release();
            }
        }
    }

    /** A deterministic-id replay is successful when the row already exists. */
    private static boolean isIdempotentDuplicate(Engine.IndexResult result) {
        Exception failure = result.getFailure();
        return failure != null
            && (failure.getClass().getSimpleName().contains("AppendOnlyIndexOperationRetryException")
                || failure.getClass().getSimpleName().contains("VersionConflictEngineException"));
    }

    private void applyLocally(MVShipStateAction.Request request, ActionListener<MVShipStateAction.Response> listener) {
        // The handler owns ONE REFERENCE on the shared batch (the same buffers
        // may be in flight to other targets): release it on every exit path,
        // never close the root directly — the last release across all
        // consumers frees the native allocation.
        MVRefCountedStateBatch shared = request.stateBatch();
        try {
            org.apache.arrow.vector.VectorSchemaRoot batch = shared.root();
            IndexShard shard = indicesService.indexServiceSafe(clusterService.state().metadata().index(request.targetIndex()).getIndex())
                .getShard(request.targetShard());
            int rows = batch.getRowCount();
            java.util.List<org.apache.arrow.vector.FieldVector> vectors = batch.getFieldVectors();
            java.util.List<String> shipFields = request.shipFields();
            if (vectors.size() != shipFields.size() && (rows != 0 || vectors.isEmpty() == false)) {
                listener.onFailure(
                    new IllegalStateException(
                        "mv ship apply: state batch has " + vectors.size() + " columns, expected " + shipFields.size()
                    )
                );
                return;
            }
            java.util.List<SourceToParse> sources = new java.util.ArrayList<>(rows);
            java.util.List<String> docIds = new java.util.ArrayList<>(rows);
            for (int row = 0; row < rows; row++) {
                java.util.Map<String, Object> doc = new java.util.HashMap<>();
                // Positional mapping — the state contract (group keys first,
                // then state columns); names in the batch carry the writer's
                // alias and are not compared.
                for (int col = 0; col < vectors.size(); col++) {
                    Object value = vectors.get(col).getObject(row);
                    if (value instanceof org.apache.arrow.vector.util.Text t) {
                        value = t.toString();
                    } else if (value instanceof Double || value instanceof Float) {
                        // Floating state values (e.g. avg's sum half) must NOT
                        // be truncated to long — the whole avg correctness
                        // rides on this bit of plumbing.
                        value = ((Number) value).doubleValue();
                    } else if (value instanceof Number n) {
                        value = n.longValue();
                    }
                    doc.put(shipFields.get(col), value);
                }
                // Normal refresh batches use source generation + row. Recovery
                // uses a stable logical range identity because catalog
                // generations can be rewritten across source recovery/merge.
                // Retrying the same missing range therefore addresses the same
                // rows even when its pinned physical files have new numbers.
                doc.put("_mv_source_generation", request.writerGeneration());
                String identity = request.batchIdentity() == null ? Long.toString(request.writerGeneration()) : request.batchIdentity();
                String docId = request.sourceIndex() + "." + request.sourceShard() + "." + identity + "." + row;
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    builder.map(doc);
                    sources.add(new SourceToParse(request.targetIndex(), docId, BytesReference.bytes(builder), MediaTypeRegistry.JSON));
                    docIds.add(docId);
                }
            }

            java.util.List<Engine.IndexResult> results = shard.applyDerivedIndexBatchOnPrimary(
                sources,
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                -1L, // UNSET_AUTO_GENERATED_TIMESTAMP
                MVShipStateTransportHandler::isIdempotentDuplicate,
                () -> {
                    // Pending claims are invisible to commits until the
                    // reentrant refresh below promotes them while publishing
                    // these exact writer files. The outer batch lock prevents
                    // another ship from interleaving apply and publication.
                    MVTargetCursorLedger.stagePending(
                        request.targetIndex(),
                        request.targetShard(),
                        request.sourceIndex(),
                        request.sourceShard(),
                        request.writerGeneration(),
                        request.foldCheckpoint(),
                        request.maxSeqNo(),
                        rows,
                        rows,
                        request.sourceCoverage()
                    );
                    if (rows == 0) {
                        // No target writer exists, so DFAE's refresh fast path
                        // will not invoke MVIndexingEngine.refresh(). Promote
                        // the coverage-only claim here while still holding the
                        // same refresh-exclusion lock; there are no rows whose
                        // visibility must be coordinated.
                        MVTargetCursorLedger.promoteAll(request.targetIndex(), request.targetShard());
                    }
                    shard.refresh("derived_state_replication");
                }
            );
            if (results.size() != rows) {
                Engine.IndexResult failure = results.isEmpty() ? null : results.get(results.size() - 1);
                String docId = results.size() < docIds.size() ? docIds.get(results.size()) : "unknown";
                listener.onFailure(
                    new IllegalStateException(
                        "mv ship atomic batch stopped before [" + docId + "]",
                        failure == null ? null : failure.getFailure()
                    )
                );
                return;
            }
            for (int row = 0; row < results.size(); row++) {
                Engine.IndexResult result = results.get(row);
                if (result.getResultType() != Engine.Result.Type.SUCCESS && isIdempotentDuplicate(result) == false) {
                    listener.onFailure(
                        new IllegalStateException(
                            "mv ship apply failed for [" + docIds.get(row) + "]: " + result.getResultType(),
                            result.getFailure()
                        )
                    );
                    return;
                }
            }
            logger.debug(
                "mv ship-apply: {} state rows into [{}][{}] ({})",
                rows,
                request.targetIndex(),
                request.targetShard(),
                transportService.getLocalNode().getId()
            );
            listener.onResponse(new MVShipStateAction.Response(rows));
        } catch (Exception e) {
            listener.onFailure(e);
        } finally {
            shared.release();
        }
    }
}
