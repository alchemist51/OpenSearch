/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.Transport;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;

public class UpdatePitController {

    private final SearchTransportService searchTransportService;
    private final ClusterService clusterService;
    private final TransportSearchAction transportSearchAction;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final PitService pitService;

    private static final Logger logger = LogManager.getLogger(UpdatePitController.class);

    @Inject
    public UpdatePitController(
        SearchTransportService searchTransportService,
        ClusterService clusterService,
        TransportSearchAction transportSearchAction,
        NamedWriteableRegistry namedWriteableRegistry,
        PitService pitService
    ){
        this.searchTransportService = searchTransportService;
        this.clusterService = clusterService;
        this.transportSearchAction = transportSearchAction;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.pitService = pitService;
    }

    public void executeUpdatePit(
        UpdatePitRequest request,
        Task task,
        StepListener<SearchResponse> updatePitListener
    ) {
        // we need a searchsource builder since the search source builder has the PIT
        //    public SearchSourceBuilder pointInTimeBuilder(PointInTimeBuilder builder) {
        //        this.pointInTimeBuilder = builder;
        //        return this;
        //    }

        // Set the Point in time ID in the search request
        SearchRequest searchRequest = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(request.getPitRequestInfo().getPitId(), request.getPitRequestInfo().getKeepAlive())));
        SearchTask searchTask = searchRequest.createTask(
            task.getId(),
            task.getType(),
            task.getAction(),
            task.getParentTaskId(),
            Collections.emptyMap()
        );
        /**
         * This is needed for cross cluster functionality to work with PITs and current ccsMinimizeRoundTrips is
         * not supported for point in time
         */
        searchRequest.setCcsMinimizeRoundtrips(false);
        /**
         * Phase 1 of create PIT
         */
        executeUpdatePit(searchTask, searchRequest, updatePitListener);

    }

    /**
     * Creates PIT reader context with temporary keep alive
     */
    void executeUpdatePit(Task task, SearchRequest searchRequest, StepListener<SearchResponse> updatePitListener) {
        logger.debug(
            () -> new ParameterizedMessage("Executing update of PIT  [{}]", searchRequest.pointInTimeBuilder().getId())
        );
        transportSearchAction.executeRequest(
            task,
            searchRequest,
            TransportUpdatePitAction.UPDATE_PIT_ACTION,
            false,
            new TransportSearchAction.SinglePhaseSearchAction() {
                @Override
                public void executeOnShardTarget(
                    SearchTask searchTask,
                    SearchShardTarget target,
                    Transport.Connection connection,
                    ActionListener<SearchPhaseResult> searchPhaseResultActionListener
                ) {
                    searchTransportService.updatePitContext(
                        connection,
                        new TransportUpdatePitAction.UpdateReaderContextRequest(
                            target.getShardId(),
                            searchRequest.pointInTimeBuilder().getKeepAlive()
                        ),
                        searchTask,
                        ActionListener.wrap(searchPhaseResultActionListener::onResponse, searchPhaseResultActionListener::onFailure)
                    );
                }
            },
            updatePitListener
        );
    }
}
