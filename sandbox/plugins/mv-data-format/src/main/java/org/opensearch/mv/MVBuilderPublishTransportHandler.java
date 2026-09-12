/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.NoShardAvailableActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.mv.pull.MVHydrateArtifactBuilder;
import org.opensearch.mv.pull.MVPullSettings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Builder-shard emulation (D1): receives a pushed publication on the node that
 * holds the FOLLOWER primary and applies it — download the named state file,
 * publish it as the next generation, answer with the applied watermark.
 * Routing is the core single-shard idiom, so a relocated follower is found
 * without the leader knowing where it moved.
 */
public final class MVBuilderPublishTransportHandler extends TransportSingleShardAction<
    MVBuilderPublishAction.Request,
    MVBuilderPublishAction.Response> {

    private final IndicesService indicesService;

    @Inject
    public MVBuilderPublishTransportHandler(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        IndicesService indicesService
    ) {
        super(
            MVBuilderPublishAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MVBuilderPublishAction.Request::new,
            ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
    }

    /** Route to the node holding the FOLLOWER shard's active primary. */
    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return state.routingTable().index(request.request().followerIndex()).shard(request.request().followerShard()).primaryShardIt();
    }

    @Override
    protected boolean resolveIndex(MVBuilderPublishAction.Request request) {
        return true;
    }

    @Override
    protected Writeable.Reader<MVBuilderPublishAction.Response> getResponseReader() {
        return MVBuilderPublishAction.Response::new;
    }

    @Override
    protected MVBuilderPublishAction.Response shardOperation(MVBuilderPublishAction.Request request, ShardId shardId) throws IOException {
        IndexShard shard = indicesService.indexServiceSafe(shardId.getIndex()).getShard(shardId.id());
        if (shard.routingEntry().primary() == false || shard.routingEntry().active() == false) {
            throw new NoShardAvailableActionException(shardId, "follower primary not active on routed node");
        }
        MVPullSettings.Services services = MVPullSettings.Services.current();
        if (services == null) {
            throw new IllegalStateException("mv_pull hydrate: plugin services not registered on this node");
        }
        return MVHydrateArtifactBuilder.applyPushed(shard, services, request);
    }
}
