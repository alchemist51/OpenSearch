/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.planner.dag.StageExecutionType;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

/**
 * Builds executions for {@link StageExecutionType#LATE_MATERIALIZATION} (QTF Scatter-Gather)
 * stages. Pulls the {@code OpenSearchLateMaterialization} marker out of the stage's fragment
 * for fetch-list metadata and hands the resulting context to {@link LateMaterializationStageExecution}.
 *
 * <p>This stage has no Substrait fragment ({@link
 * org.opensearch.analytics.planner.dag.FragmentConversionDriver} skips it), so unlike
 * {@link LocalStageScheduler} we do not pull plan bytes / sink providers / instructions
 * from the stage. The wrapper RelNode itself carries everything we need: the fetch list
 * (which columns to fetch) and per-column storage info (how the data-node should read them).
 *
 * @opensearch.internal
 */
final class LateMaterializationStageScheduler implements StageScheduler {

    private final ClusterService clusterService;
    private final AnalyticsSearchTransportService transport;

    LateMaterializationStageScheduler(ClusterService clusterService, AnalyticsSearchTransportService transport) {
        this.clusterService = clusterService;
        this.transport = transport;
    }

    @Override
    public StageExecution createExecution(Stage stage, ExchangeSink sink, QueryContext config) {
        // The wrapper RelNode is in stage.getFragment() — the StageExecution finds it via
        // RelNodeUtils.findNode(fragment, OpenSearchLateMaterialization.class) and reads
        // fetchList + fetchListStorage off it. Keep wrapper-extraction out of the scheduler
        // so the execution class is self-contained for unit testing.
        return new LateMaterializationStageExecution(stage, config, sink, clusterService, transport);
    }
}
