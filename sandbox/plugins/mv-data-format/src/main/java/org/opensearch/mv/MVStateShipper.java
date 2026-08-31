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
 * Colocated Arrow implementation of {@link DerivedStateReplicator}. A source
 * refresh hands it a finalized state batch; it applies the batch to the
 * ordinal-paired derived target and waits until that state is searchable.
 *
 * <p>The source-to-target hop is currently zero-copy Arrow C Data because
 * colocation keeps both primaries in one JVM. This is not a bulk-document
 * protocol. A future non-colocated implementation should serialize the same
 * batch as Arrow IPC or Arrow Flight while retaining these coordinates and
 * acknowledgement semantics.
 *
 * <p>Ship-before-refresh: any failure throws and the source refresh does not
 * complete. Target durability advances independently only after a later
 * source commit asynchronously caps the exact published claim; source-refresh
 * reconciliation repairs any target state lost before that target commit.
 */
final class MVStateShipper implements DerivedStateReplicator {

    private static final Logger logger = LogManager.getLogger(MVStateShipper.class);

    private final Client client;
    private final java.util.List<String> targetIndices;
    private final String sourceIndex;
    private final ShardId shardId;
    private final org.opensearch.cluster.service.ClusterService clusterService;
    private final MVDefinitionSpec spec;

    MVStateShipper(
        Client client,
        java.util.List<String> targetIndices,
        String sourceIndex,
        ShardId shardId,
        org.opensearch.cluster.service.ClusterService clusterService,
        MVDefinitionSpec spec
    ) {
        this.spec = spec;
        this.client = client;
        this.targetIndices = java.util.List.copyOf(targetIndices);
        this.sourceIndex = sourceIndex;
        this.shardId = shardId;
        this.clusterService = clusterService;
    }

    /** Ships source-operation coverage that produces no derived rows (certified no-ops). */
    long replicateCoverageOnly(BatchCoordinates coordinates) throws IOException {
        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            org.apache.arrow.vector.VectorSchemaRoot empty = org.apache.arrow.vector.VectorSchemaRoot.create(
                new org.apache.arrow.vector.types.pojo.Schema(java.util.List.of()),
                allocator
            );
            empty.setRowCount(0);
            // replicate() transfers the root to the synchronous local handler,
            // which releases it before this call returns.
            return replicate(empty, coordinates);
        }
    }

    /**
     * Ships the finalized live Arrow state batch to every configured target's
     * ordinal-paired shard, sharing one copy of the buffers through
     * {@link MVRefCountedStateBatch}. Each target owns and releases one
     * reference; the final release frees the native allocation.
     *
     * <p>Publication gate: every target must acknowledge the full row count as
     * searchable. Any failure fails the source refresh. Durable target
     * certification happens later under the asynchronous source-commit cap.
     *
     * @return number of state rows shipped per target
     */
    @Override
    public long replicate(VectorSchemaRoot stateBatch, BatchCoordinates coordinates) throws IOException {
        long writerGeneration = coordinates.writerGeneration();
        long foldCheckpoint = coordinates.foldCheckpoint();
        long batchMaxSeqNo = coordinates.maxSeqNo();
        String definition = coordinates.definitionFingerprint();
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
                foldCheckpoint,
                batchMaxSeqNo,
                definition,
                coordinates.batchIdentity(),
                coordinates.sourceCoverage(),
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
            // Full row-count acknowledgement is the searchable publication gate.
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
            logger.info(
                "mv replicate: gen={} applied {} state rows from {}[{}] -> [{}][{}] (acked searchable)",
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
