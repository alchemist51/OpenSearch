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
 * Applies shipped MV state rows on the local TARGET primary through the
 * shard's write path: each row becomes an index operation with an external
 * doc id (idempotent overwrite on re-ship), written to the translog, and the
 * translog is fsynced before the ack — <b>the ack means durable</b>. Handing
 * the buffer to the writer around the write path would break the invariant on
 * a target crash (acked-but-lost state); this is decision 13 in the
 * separate-index README.
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
            if (vectors.size() != shipFields.size()) {
                listener.onFailure(
                    new IllegalStateException(
                        "mv ship apply: state batch has " + vectors.size() + " columns, expected " + shipFields.size()
                    )
                );
                return;
            }
            for (int row = 0; row < rows; row++) {
                java.util.Map<String, Object> doc = new java.util.HashMap<>();
                // Positional mapping — the state contract (group keys first,
                // then state columns); names in the batch carry the writer's
                // alias and are not compared.
                for (int col = 0; col < vectors.size(); col++) {
                    Object value = vectors.get(col).getObject(row);
                    if (value instanceof org.apache.arrow.vector.util.Text t) {
                        value = t.toString();
                    } else if (value instanceof Number n) {
                        value = n.longValue();
                    }
                    doc.put(shipFields.get(col), value);
                }
                // Provenance (decision 21): ONE field. Idempotency lives in the
                // deterministic _id (source.shard.gen.row); source index+shard are
                // constants per target shard under ordinal-paired colocation. The
                // generation field alone has a query job — the orphan sweep's
                // delete-by-generation.
                doc.put("_mv_source_generation", request.writerGeneration());
                String docId = request.sourceIndex() + "." + request.sourceShard() + "." + request.writerGeneration() + "." + row;
                try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                    builder.map(doc);
                    SourceToParse source = new SourceToParse(
                        request.targetIndex(),
                        docId,
                        BytesReference.bytes(builder),
                        MediaTypeRegistry.JSON
                    );
                    Engine.IndexResult result = shard.applyIndexOperationOnPrimary(
                        Versions.MATCH_ANY,
                        VersionType.INTERNAL,
                        source,
                        org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO,
                        0,
                        -1L, // UNSET_AUTO_GENERATED_TIMESTAMP
                        false
                    );
                    if (result.getResultType() != Engine.Result.Type.SUCCESS) {
                        // Idempotent re-ship vs APPEND-ONLY composite target:
                        // recovery replay re-ships a generation whose rows may
                        // already exist under the same deterministic doc id.
                        // Presence satisfies the invariant — tolerate-duplicate
                        // IS the idempotency on an append-only index (overwrite
                        // is impossible there). Divergent stale-orphan content
                        // is the generation-watermark sweep's job (designed,
                        // pending). POC-accepted risk until the sweep lands.
                        Exception failure = result.getFailure();
                        boolean alreadyExists = failure != null
                            && (failure.getClass().getSimpleName().contains("AppendOnlyIndexOperationRetryException")
                                || failure.getClass().getSimpleName().contains("VersionConflictEngineException"));
                        if (alreadyExists) {
                            continue; // row present => counts toward the ack
                        }
                        listener.onFailure(
                            new IllegalStateException(
                                "mv ship apply failed for [" + docId + "]: " + result.getResultType(),
                                result.getFailure()
                            )
                        );
                        return;
                    }
                }
            }
            // Durability before ack: fsync the translog to the last applied op.
            shard.sync();
            // Searchability before ack (design contract, README "current
            // understanding" step 4-5): the ack certifies BOTH durable and
            // searchable, so when the source commits (step 6) the target's
            // latest view already supersets any source view a query can hold —
            // the whole consistency story, no snapshot mapping needed.
            shard.refresh("mv_ship");
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
