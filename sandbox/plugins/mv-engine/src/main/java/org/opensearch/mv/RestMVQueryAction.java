/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * REST handler: {@code POST /_mv/{target}/_query}
 *
 * <p>POC direct-query endpoint. Executes the MV definition's Final aggregation
 * over the target's hydrated partial state files, returning the exact answer
 * rows as JSON.</p>
 *
 * <p>Production note: this would be replaced by transparent routing inside the
 * SearchBackEndPlugin/analytics-engine, where MV target queries auto-route
 * through the DataFusion fold without a separate REST action. The REST action
 * stays for POC testing and debugging.</p>
 *
 * @opensearch.api
 */
public class RestMVQueryAction extends BaseRestHandler {

    private final MVReadService readService;

    public RestMVQueryAction(MVReadService readService) {
        this.readService = readService;
    }

    @Override
    public String getName() {
        return "mv_query_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/_mv/{target}/_query"),
            new Route(RestRequest.Method.GET, "/_mv/{target}/_query")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String targetIndex = request.param("target");
        // POC: shard data path passed as query param (in production, resolved from shard routing).
        String shardDataPath = request.param("shard_data_path");
        if (shardDataPath == null || shardDataPath.isEmpty()) {
            return channel -> {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("error", "shard_data_path query parameter required (POC — production resolves from shard routing)");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.BAD_REQUEST, builder));
            };
        }

        return channel -> {
            try {
                MVReadService.MVQueryResult result = readService.query(targetIndex, Path.of(shardDataPath));
                XContentBuilder builder = channel.newBuilder();
                result.toXContent(builder);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                XContentBuilder builder = channel.newBuilder();
                builder.startObject();
                builder.field("error", e.getMessage());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
            }
        };
    }
}
