/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;

public class TransportUpdatePitAction extends HandledTransportAction<UpdatePitRequest, UpdatePitResponse> {

    public static final String UPDATE_PIT_ACTION = "update_pit";
    private final TransportService transportService;
    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final UpdatePitController updatePitController;

    @Inject
    public TransportUpdatePitAction(
        TransportService transportService,
        ActionFilters actionFilters,
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        UpdatePitController updatePitController
    ) {
        super(UpdatePitAction.NAME, transportService, actionFilters, in -> new UpdatePitRequest(in));
        this.transportService = transportService;
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.updatePitController = updatePitController;
    }

    @Override
    protected void doExecute(Task task, UpdatePitRequest request, ActionListener<UpdatePitResponse> listener) {
        final StepListener<SearchResponse> updatePitListener = new StepListener<>();
        updatePitController.executeUpdatePit(request, task, updatePitListener);
    }

    public static class UpdateReaderContextRequest extends TransportRequest {
        private final ShardId shardId;
        private final TimeValue keepAlive;

        public UpdateReaderContextRequest(ShardId shardId, TimeValue keepAlive){
            this.shardId = shardId;
            this.keepAlive = keepAlive;
        }

        public ShardId getShardId() { return shardId;}

        public TimeValue getKeepAlive() {
            return keepAlive;
        }

        public UpdateReaderContextRequest(StreamInput in) throws IOException {
            super(in);
            this.shardId = new ShardId(in);
            this.keepAlive = in.readTimeValue();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeTimeValue(keepAlive);
        }
    }

    public static class UpdateReaderContextResponse extends SearchPhaseResult {
        public UpdateReaderContextResponse(ShardSearchContextId shardSearchContextId){
            this.contextId = shardSearchContextId;
        }

        public UpdateReaderContextResponse(StreamInput in) throws IOException {
            super(in);
            contextId = new ShardSearchContextId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            contextId.writeTo(out);
        }
    }
}
