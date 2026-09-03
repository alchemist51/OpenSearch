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
 * Receives a source-pushed checkpoint advert and deposits it into the
 * node-local {@link MVCheckpointMailbox}. The target shard's poller
 * consumes the mailbox on its next round instead of doing expensive
 * remote-store listing.
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
                // Plugin not yet initialized — reject gracefully; source will retry.
                listener.onResponse(new MVCheckpointPublishAction.Response(false, -1L));
                return;
            }

            MVCheckpointMailbox.PushedAdvert advert = new MVCheckpointMailbox.PushedAdvert(
                request.sourceIndex(),
                request.sourceUuid(),
                request.sourceShard(),
                request.maxSeqNo(),
                request.primaryTerm(),
                request.infosVersion(),
                request.parquetFiles(),
                request.fileSizes(),
                request.fileMinSeqNos(),
                request.fileMaxSeqNos(),
                System.nanoTime()
            );

            mailbox.deliver(request.targetIndex(), request.targetShard(), advert);

            // Return the target's current watermark from the mailbox.
            // The mailbox tracks the highest consumed maxSeqNo — which is the
            // most recently processed advert's maxSeqNo. If no adverts have been
            // consumed yet (target poller hasn't run), we peek the last-consumed
            // highwater from the mailbox's metadata. For now, use -1 (unknown)
            // as a correct initial value — the watermark will be populated from
            // the target's DerivedShardPoller state in a follow-up when the poller
            // reports back. This is safe: -1 means "send full list" (fail-open).
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
