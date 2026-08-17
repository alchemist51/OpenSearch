/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.client.Client;

import java.io.IOException;

/**
 * POC(mv) separate-index ship path: exports a finalized state file's rows and
 * synchronously bulk-indexes them into the target MV index.
 *
 * <p><b>Ship-before-commit</b>: the caller ({@code MVWriter.flush}) invokes
 * {@link #ship} BEFORE returning its flush result; any failure here throws,
 * the flush fails, and the source does not commit — the data-level invariant
 * (committed-on-source ⇒ present-on-target) holds by construction.
 *
 * <p><b>Idempotence</b>: doc IDs are deterministic
 * ({@code <sourceIndex>.<shard>.<generation>.<row>}), so a retried flush
 * (after a failed ship) overwrites rather than duplicates — re-shipping the
 * same generation is safe.
 *
 * <p><b>Visibility</b>: refresh policy NONE — ship+ack guarantees durable
 * presence; rows become searchable after the MV index's own refresh (design
 * decision 6 in the separate-index README).
 *
 * <p>POC simplifications (documented in the separate-index folder): the state
 * rows travel as parsed TSV from the native export rather than Arrow batches
 * over a dedicated transport action; the synchronous bulk blocks the flushing
 * thread (production wants a ship executor + ack listener); no colocation
 * fast path (that is the resize-precedent Phase 1/2 work).
 */
final class MVStateShipper {

    private static final Logger logger = LogManager.getLogger(MVStateShipper.class);

    private final Client client;
    private final String targetIndex;
    private final String sourceIndex;
    private final ShardId shardId;
    private final org.opensearch.cluster.service.ClusterService clusterService;

    MVStateShipper(
        Client client,
        String targetIndex,
        String sourceIndex,
        ShardId shardId,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        this.client = client;
        this.targetIndex = targetIndex;
        this.sourceIndex = sourceIndex;
        this.shardId = shardId;
        this.clusterService = clusterService;
    }

    /**
     * Ships every state row of {@code stateFile} to the target MV index and
     * blocks until the bulk is acked. Throws on ANY failure — the caller must
     * fail the flush (ship-before-commit).
     *
     * @return number of state rows shipped
     */
    /**
     * Ships the finalized state batch — the LIVE Arrow root, zero copies since
     * the native writer produced it — to the ordinal-paired target shard and
     * blocks until the durable+searchable ack. Throws on ANY failure — the
     * caller must fail the flush (ship-before-commit). The handler owns and
     * closes the root.
     *
     * @return number of state rows shipped
     */
    long ship(VectorSchemaRoot stateBatch, long writerGeneration) throws IOException {
        int rows = stateBatch.getRowCount();
        int targetShard = shardId.id() % clusterService.state().metadata().index(targetIndex).getNumberOfShards();
        MVShipStateAction.Request request = new MVShipStateAction.Request(
            targetIndex,
            targetShard,
            sourceIndex,
            shardId.id(),
            writerGeneration,
            stateBatch
        );
        MVShipStateAction.Response response;
        try {
            response = client.execute(MVShipStateAction.INSTANCE, request).actionGet();
        } catch (Exception e) {
            throw new IOException(
                "mv ship: transport ship to [" + targetIndex + "][" + targetShard + "] failed gen=" + writerGeneration,
                e
            );
        }
        // rowsReceived verification = the commit gate (challenges §10): the ack
        // returns the applied count; a mismatch means the target does not hold
        // this generation's complete state — the flush must not commit.
        if (response.applied() != rows) {
            throw new IOException(
                "mv ship: gen=" + writerGeneration + " shipped " + rows + " state rows but target applied " + response.applied()
            );
        }
        logger.info(
            "mv ship: gen={} shipped {} state rows from {}[{}] -> [{}][{}] (acked durable+searchable)",
            writerGeneration,
            response.applied(),
            sourceIndex,
            shardId.id(),
            targetIndex,
            targetShard
        );
        return response.applied();
    }
}
