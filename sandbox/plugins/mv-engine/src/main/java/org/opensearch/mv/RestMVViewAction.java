/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

/**
 * REST handler for the MV view control plane:
 * <ul>
 *   <li>{@code PUT /_mv/views/{name}} — validate + create the derived target
 *       (poller starts via the existing lifecycle);</li>
 *   <li>{@code GET /_mv/views/{name}} — describe the target's binding +
 *       descriptor summary;</li>
 *   <li>{@code DELETE /_mv/views/{name}} — delete the target index (stopping its
 *       poller through the derived-pull lifecycle listener).</li>
 * </ul>
 *
 * <p>{@code name} is the target index name for {@code GET}/{@code DELETE}; for
 * {@code PUT} it is the view name and also the default target index (overridable
 * via the {@code target_index} body field).
 */
public class RestMVViewAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "mv_view_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_mv/views/{name}"),
            new Route(RestRequest.Method.GET, "/_mv/views/{name}"),
            new Route(RestRequest.Method.DELETE, "/_mv/views/{name}")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String name = request.param("name");
        return switch (request.method()) {
            case PUT -> preparePut(name, request, client);
            case GET -> channel -> client.execute(
                MVGetViewAction.INSTANCE,
                new MVGetViewRequest(name),
                new RestStatusToXContentListener<>(channel)
            );
            case DELETE -> channel -> client.admin()
                .indices()
                .delete(new DeleteIndexRequest(name), new RestToXContentListener<>(channel));
            default -> throw new IllegalArgumentException("unsupported method [" + request.method() + "] for /_mv/views/{name}");
        };
    }

    private RestChannelConsumer preparePut(String name, RestRequest request, NodeClient client) throws IOException {
        final MVCreateViewRequest createRequest;
        try (XContentParser parser = request.contentParser()) {
            createRequest = MVCreateViewRequest.fromXContent(name, parser);
        }
        return channel -> client.execute(
            MVCreateViewAction.INSTANCE,
            createRequest,
            new RestStatusToXContentListener<>(channel, r -> "/_mv/views/" + r.targetIndex())
        );
    }
}
