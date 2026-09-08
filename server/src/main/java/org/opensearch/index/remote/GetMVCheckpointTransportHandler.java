/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;

/**
 * Source-side transport handler for {@link MVCheckpointService#ACTION_GET_CHECKPOINT}.
 *
 * <p>Registered directly on {@link TransportService#registerRequestHandler} with the
 * internal action name. This is the defect-#19 fix pattern: the handler runs on
 * the GENERIC thread pool on the node holding the source primary. The request is
 * routed to the correct node by the caller (target hydrator) using cluster state
 * routing.</p>
 *
 * <p>Handler body: reads the per-shard {@link MVCheckpoint} from
 * {@link MVCheckpointService#getCheckpoint} — the checkpoint is derived ONLY
 * from remote-committed manifests (defect-#30 invariant).</p>
 *
 * @opensearch.internal
 */
public final class GetMVCheckpointTransportHandler implements TransportRequestHandler<GetMVCheckpointRequest> {

    private static final Logger logger = LogManager.getLogger(GetMVCheckpointTransportHandler.class);

    private GetMVCheckpointTransportHandler() {}

    /**
     * Register this handler on the given transport service. Call once at node startup
     * (from the plugin's createComponents or equivalent wiring site).
     */
    public static void register(TransportService transportService) {
        transportService.registerRequestHandler(
            MVCheckpointService.ACTION_GET_CHECKPOINT,
            ThreadPool.Names.GENERIC,
            GetMVCheckpointRequest::new,
            new GetMVCheckpointTransportHandler()
        );
        logger.info("Registered GetMVCheckpoint transport handler on action={}", MVCheckpointService.ACTION_GET_CHECKPOINT);
    }

    @Override
    public void messageReceived(GetMVCheckpointRequest request, TransportChannel channel, Task task) throws Exception {
        ShardId shardId = request.shardId();
        MVCheckpoint checkpoint = MVCheckpointService.getCheckpoint(shardId);
        logger.debug(
            "CHECKPOINT_REPLY shard={} term={} maxSeqNo={} mvs={}",
            shardId,
            checkpoint.primaryTerm(),
            checkpoint.maxSeqNo(),
            checkpoint.entries().size()
        );
        channel.sendResponse(new GetMVCheckpointResponse(checkpoint));
    }
}
