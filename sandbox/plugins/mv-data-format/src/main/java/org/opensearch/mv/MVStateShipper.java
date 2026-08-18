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
    private final java.util.List<String> targetIndices;
    private final String sourceIndex;
    private final ShardId shardId;
    private final org.opensearch.cluster.service.ClusterService clusterService;
    private final MVDefinitionSpec spec;
    /** Engine-owned per-target high-water of acked catalog snapshot versions (commit sync, decision 25). */
    private final java.util.concurrent.ConcurrentMap<String, Long> targetSnapshotHighWater;

    MVStateShipper(
        Client client,
        java.util.List<String> targetIndices,
        String sourceIndex,
        ShardId shardId,
        org.opensearch.cluster.service.ClusterService clusterService,
        MVDefinitionSpec spec,
        java.util.concurrent.ConcurrentMap<String, Long> targetSnapshotHighWater
    ) {
        this.spec = spec;
        this.client = client;
        this.targetIndices = java.util.List.copyOf(targetIndices);
        this.sourceIndex = sourceIndex;
        this.shardId = shardId;
        this.clusterService = clusterService;
        this.targetSnapshotHighWater = targetSnapshotHighWater;
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
    /**
     * Ships the finalized state batch to EVERY configured target's ordinal-
     * paired shard, sharing ONE copy of the Arrow buffers across all of them
     * via {@link MVRefCountedStateBatch}: the source acquires one reference
     * per target up front; each target's handler releases its own; if the
     * ship to a target fails before its handler took ownership, the source
     * releases that reference here. The last release — wherever it happens —
     * frees the native memory. No destination can free the batch under
     * another.
     *
     * <p>Commit gate: EVERY target must ack with the full row count; any
     * failure fails the flush (ship-before-commit, invariant per target).
     *
     * @return number of state rows shipped (per target)
     */
    long ship(VectorSchemaRoot stateBatch, long writerGeneration) throws IOException {
        int rows = stateBatch.getRowCount();
        MVRefCountedStateBatch shared = new MVRefCountedStateBatch(stateBatch, targetIndices.size());
        IOException failure = null;
        for (String targetIndex : targetIndices) {
            if (failure != null) {
                // A previous target already failed the flush: don't ship to the
                // remaining targets this attempt — just release their refs.
                shared.release();
                continue;
            }
            int targetShard = shardId.id() % clusterService.state().metadata().index(targetIndex).getNumberOfShards();
            MVShipStateAction.Request request = new MVShipStateAction.Request(
                targetIndex,
                targetShard,
                sourceIndex,
                shardId.id(),
                writerGeneration,
                spec.shipFields(),
                shared
            );
            MVShipStateAction.Response response = null;
            try {
                response = client.execute(MVShipStateAction.INSTANCE, request).actionGet();
            } catch (Exception e) {
                // The handler releases its reference on every path it reaches;
                // dispatch happens synchronously in-JVM (NodeClient), so a
                // throw here means the handler's finally already ran or the
                // request never left this method — either way OUR contract is
                // one release per target, and the handler owns it once
                // doExecute is entered. dispatch-failure-before-doExecute is
                // not a real path for NodeClient; treat the ref as consumed.
                failure = new IOException(
                    "mv ship: transport ship to [" + targetIndex + "][" + targetShard + "] failed gen=" + writerGeneration,
                    e
                );
                continue;
            }
            // rowsReceived verification = the commit gate (challenges §10).
            if (response.applied() != rows) {
                failure = new IOException(
                    "mv ship: gen="
                        + writerGeneration
                        + " shipped "
                        + rows
                        + " state rows to ["
                        + targetIndex
                        + "] but it applied "
                        + response.applied()
                );
                continue;
            }
            if (response.snapshotVersion() >= 0) {
                targetSnapshotHighWater.merge(targetIndex, response.snapshotVersion(), Math::max);
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
        }
        if (failure != null) {
            throw failure;
        }
        return rows;
    }
}
