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
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.transport.RemoteClusterService;

import java.util.Set;
import java.util.function.BiFunction;

/**
 * Helper class for common search functions
 */
public class SearchUtils {

    private static final Logger logger = LogManager.getLogger(SearchUtils.class);
    public SearchUtils() {}

    /**
     * Get connection lookup listener for list of clusters passed
     */
    public static ActionListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(
        RemoteClusterService remoteClusterService,
        ClusterState state,
        Set<String> clusters
    ) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty()) {

            lookupListener.onResponse((cluster, nodeId) -> {
                logger.info("cluster"+ cluster);
                logger.info("lookup Listneer"+ nodeId);
                return state.getNodes().get(nodeId);
            });
        } else {
            logger.info("clusters is not empty");
            remoteClusterService.collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }
}
