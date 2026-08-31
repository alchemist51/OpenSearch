/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Answers {@link MVCursorAction} with the target shard's live exact published
 * claim. On engine open the ledger is seeded from the target's own durable
 * commit; post-open publication may advance the live claim before the next
 * commit. After a target-only restart, the live claim therefore falls back to
 * durable metadata and the next source refresh ships only its exact missing
 * complement.
 */
public final class MVCursorTransportHandler extends HandledTransportAction<MVCursorAction.Request, MVCursorAction.Response> {

    private final org.opensearch.cluster.service.ClusterService clusterService;

    @Inject
    public MVCursorTransportHandler(
        TransportService transportService,
        ActionFilters actionFilters,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        super(MVCursorAction.NAME, transportService, actionFilters, MVCursorAction.Request::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MVCursorAction.Request request, ActionListener<MVCursorAction.Response> listener) {
        var indexRouting = clusterService.state().routingTable().index(request.targetIndex());
        if (indexRouting == null) {
            listener.onFailure(new IllegalStateException("mv cursor: target [" + request.targetIndex() + "] does not exist"));
            return;
        }
        var primary = indexRouting.shard(request.targetShard()).primaryShard();
        if (primary == null || primary.active() == false || primary.currentNodeId() == null) {
            listener.onFailure(
                new IllegalStateException(
                    "mv cursor: target primary [" + request.targetIndex() + "][" + request.targetShard() + "] is not active"
                )
            );
            return;
        }
        if (primary.currentNodeId().equals(clusterService.localNode().getId()) == false) {
            listener.onFailure(
                new IllegalStateException(
                    "mv cursor: target primary ["
                        + request.targetIndex()
                        + "]["
                        + request.targetShard()
                        + "] is not local; colocation is required"
                )
            );
            return;
        }
        MVTargetCursorLedger.Cursor cursor = MVTargetCursorLedger.certified(
            request.targetIndex(),
            request.targetShard(),
            request.sourceIndex(),
            request.sourceShard()
        );
        MVSourceSeqCoverage coverage = MVTargetCursorLedger.certifiedCoverage(
            request.targetIndex(),
            request.targetShard(),
            request.sourceIndex(),
            request.sourceShard()
        );
        listener.onResponse(new MVCursorAction.Response(cursor.certifiedGeneration(), cursor.checkpoint(), coverage));
    }
}
