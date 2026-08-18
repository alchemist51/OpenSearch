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
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Applies a commit-sync request on the LOCAL target primary (decision 25):
 * flushes the target shard — committing its latest catalog snapshot, which
 * is at least the version the preceding ship acks reported — and responds
 * with the committed version. Runs synchronously on the calling thread
 * (SAME executor): the caller IS the source's flush thread inside its
 * commit section, and the target flush must complete before the source
 * commit proceeds; a pool hop would only add deadlock surface (the
 * write-pool lesson from ship-before-commit).
 *
 * <p>Same hard locality rule as the ship handler: the pair is colocated,
 * a non-local target primary fails the request (and with it the source's
 * flush) rather than falling back to remote.
 */
public final class MVCommitSyncTransportHandler extends HandledTransportAction<MVCommitSyncAction.Request, MVCommitSyncAction.Response> {

    private static final Logger logger = LogManager.getLogger(MVCommitSyncTransportHandler.class);

    private final IndicesService indicesService;
    private final org.opensearch.cluster.service.ClusterService clusterService;

    @Inject
    public MVCommitSyncTransportHandler(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        super(MVCommitSyncAction.NAME, transportService, actionFilters, MVCommitSyncAction.Request::new, ThreadPool.Names.SAME);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MVCommitSyncAction.Request request, ActionListener<MVCommitSyncAction.Response> listener) {
        try {
            org.opensearch.cluster.routing.ShardRouting primary = clusterService.state()
                .routingTable()
                .index(request.targetIndex())
                .shard(request.targetShard())
                .primaryShard();
            if (primary == null || primary.active() == false || primary.currentNodeId() == null) {
                listener.onFailure(
                    new IllegalStateException(
                        "mv commit sync: target primary [" + request.targetIndex() + "][" + request.targetShard() + "] is not active"
                    )
                );
                return;
            }
            if (primary.currentNodeId().equals(clusterService.localNode().getId()) == false) {
                // Hard locality rule — split pair refuses the source commit.
                listener.onFailure(
                    new IllegalStateException(
                        "mv commit sync: target primary ["
                            + request.targetIndex()
                            + "]["
                            + request.targetShard()
                            + "] is not local (colocation violated) — refusing the source commit"
                    )
                );
                return;
            }
            ShardId shardId = new ShardId(clusterService.state().metadata().index(request.targetIndex()).getIndex(), request.targetShard());
            IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
            // Commit the target's latest catalog snapshot. The engine skips
            // the write when the snapshot id is already committed — repeated
            // source flushes with no new generations stay cheap.
            shard.flush(new FlushRequest().force(false).waitIfOngoing(true));
            long committed = shard.compositeCatalogSnapshotVersion();
            if (committed >= 0 && committed < request.minVersion()) {
                listener.onFailure(
                    new IllegalStateException(
                        "mv commit sync: target ["
                            + request.targetIndex()
                            + "] committed version "
                            + committed
                            + " < required "
                            + request.minVersion()
                    )
                );
                return;
            }
            logger.debug(
                "mv commit sync: [{}][{}] committed catalog version {} (required >= {})",
                request.targetIndex(),
                request.targetShard(),
                committed,
                request.minVersion()
            );
            listener.onResponse(new MVCommitSyncAction.Response(committed));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
