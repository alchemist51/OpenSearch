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
import org.opensearch.core.action.ActionListener;
import org.opensearch.mv.pull.MVCheckpointMailbox;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Receives a source-pushed checkpoint and deposits the
 * {@link MVReplicationCheckpoint} into the node-local
 * {@link MVCheckpointMailbox}. The target shard's poller consumes the
 * mailbox on its next round instead of doing expensive remote-store listing.
 *
 * <p>Runs on the GENERIC thread pool. The handler does not touch any
 * IndexShard state — it only writes to the lock-free mailbox.
 *
 * <p>Unlike the ship-state handler, this action is wire-serializable
 * and works across nodes (pull model does not require colocation).
 */
public final class MVCheckpointPublishTransportHandler extends HandledTransportAction<
    MVCheckpointPublishAction.Request,
    MVCheckpointPublishAction.Response> {

    private static final Logger logger = LogManager.getLogger(MVCheckpointPublishTransportHandler.class);

    @Inject
    public MVCheckpointPublishTransportHandler(TransportService transportService, ActionFilters actionFilters) {
        super(
            MVCheckpointPublishAction.NAME,
            transportService,
            actionFilters,
            MVCheckpointPublishAction.Request::new,
            ThreadPool.Names.GENERIC
        );
    }

    @Override
    protected void doExecute(
        Task task,
        MVCheckpointPublishAction.Request request,
        ActionListener<MVCheckpointPublishAction.Response> listener
    ) {
        try {
            MVCheckpointMailbox mailbox = MVCheckpointMailbox.instance();
            if (mailbox == null) {
                listener.onResponse(new MVCheckpointPublishAction.Response(false, -1L));
                return;
            }

            mailbox.deliver(request.targetIndex(), request.targetShard(), request.checkpoint());

            long targetWatermark = mailbox.lastConsumedWatermark(
                request.targetIndex(), request.targetShard(),
                request.sourceIndex(), request.sourceShard()
            );

            listener.onResponse(new MVCheckpointPublishAction.Response(true, targetWatermark));
        } catch (Exception e) {
            logger.warn(
                "checkpoint_publish: failed to deliver advert for target=[{}][{}] source=[{}][{}]",
                request.targetIndex(),
                request.targetShard(),
                request.sourceIndex(),
                request.sourceShard(),
                e
            );
            listener.onFailure(e);
        }
    }
}
