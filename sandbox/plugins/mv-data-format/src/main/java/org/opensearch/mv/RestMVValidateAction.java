/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

/**
 * REST handler for {@code POST /_mv/_validate}. Dry-run compiles and validates a
 * materialized-view definition against a source index's real mapping; never
 * creates or mutates any index.
 *
 * <p>Returns {@code 200} with the compiled descriptor, {@code state_fields},
 * {@code native_state_fields}, {@code target_mapping}, and {@code ordering} on a
 * valid definition; {@code 400} with a {@code reason_code}, message, and any
 * schema {@code mismatches} on rejection.
 */
public class RestMVValidateAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "mv_validate_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_mv/_validate"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MVValidateRequest validateRequest;
        try (XContentParser parser = request.contentParser()) {
            validateRequest = MVValidateRequest.fromXContent(parser);
        }
        return channel -> client.execute(MVValidateAction.INSTANCE, validateRequest, new RestStatusToXContentListener<>(channel));
    }
}
