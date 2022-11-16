/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.UpdatePitRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestUpdatePitAction extends BaseRestHandler {

    @Override
    public String getName(){
        return "update_pit_action";
    }

    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String pit_id = request.param("pit_id");
        UpdatePitRequest updatePitRequest = new UpdatePitRequest(pit_id);

        request.withContentOrSourceParamParserOrNull((xContentParser -> {
            if(xContentParser!=null){
                try{
                    updatePitRequest.fromXContent(xContentParser);
                } catch (IOException e) {
                    throw new IllegalArgumentException("Failed to parse request body", e);
                }
            }
        }));

        return channel -> client.
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(POST, "/_search/point_in_time/{pit_id}")));
    }
}
