/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/** Applies an asynchronous source-commit cap and commits the eligible target claim. */
public final class MVSourceCommitTransportHandler extends HandledTransportAction<
    MVSourceCommitAction.Request,
    MVSourceCommitAction.Response> {

    private final IndicesService indicesService;
    private final org.opensearch.cluster.service.ClusterService clusterService;

    @Inject
    public MVSourceCommitTransportHandler(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        super(MVSourceCommitAction.NAME, transportService, actionFilters, MVSourceCommitAction.Request::new, ThreadPool.Names.FLUSH);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MVSourceCommitAction.Request request, ActionListener<MVSourceCommitAction.Response> listener) {
        try {
            var metadata = clusterService.state().metadata().index(request.targetIndex());
            if (metadata == null) {
                throw new IllegalStateException("mv source commit: target [" + request.targetIndex() + "] does not exist");
            }
            var primary = clusterService.state().routingTable().index(request.targetIndex()).shard(request.targetShard()).primaryShard();
            if (primary == null || primary.active() == false || primary.currentNodeId() == null) {
                throw new IllegalStateException(
                    "mv source commit: target primary [" + request.targetIndex() + "][" + request.targetShard() + "] is not active"
                );
            }
            if (primary.currentNodeId().equals(clusterService.localNode().getId()) == false) {
                throw new IllegalStateException("mv source commit: target primary is not colocated");
            }
            IndexShard shard = indicesService.indexServiceSafe(metadata.getIndex()).getShard(request.targetShard());
            MVTargetCursorLedger.advanceSourceCommitCap(
                request.targetIndex(),
                request.targetShard(),
                request.sourceIndex(),
                request.sourceShard(),
                request.committedCheckpoint()
            );
            shard.flush(new FlushRequest(request.targetIndex()).force(true).waitIfOngoing(true));
            listener.onResponse(new MVSourceCommitAction.Response(request.committedCheckpoint()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
