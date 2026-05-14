/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.exec.stage.FetchStageExecution;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.core.action.ActionListener;

import java.util.List;
import java.util.function.Supplier;

/**
 * QTF completion listener: detects query-then-fetch results and delegates
 * to {@link FetchStageExecution} for the fetch + assembly phase.
 * Non-QTF queries pass through unchanged.
 */
public class QTFCompletionListener implements ActionListener<Iterable<VectorSchemaRoot>> {

    private final ActionListener<Iterable<VectorSchemaRoot>> realListener;
    private final String queryId;
    private final AnalyticsSearchTransportService dispatcher;
    private final BufferAllocator allocator;
    private final Supplier<List<ShardExecutionTarget>> shardTargetsSupplier;
    private final org.opensearch.analytics.exec.task.AnalyticsQueryTask parentTask;

    public QTFCompletionListener(
        ActionListener<Iterable<VectorSchemaRoot>> realListener,
        String queryId,
        AnalyticsSearchTransportService dispatcher,
        BufferAllocator allocator,
        Supplier<List<ShardExecutionTarget>> shardTargetsSupplier,
        org.opensearch.analytics.exec.task.AnalyticsQueryTask parentTask
    ) {
        this.realListener = realListener;
        this.queryId = queryId;
        this.dispatcher = dispatcher;
        this.allocator = allocator;
        this.shardTargetsSupplier = shardTargetsSupplier;
        this.parentTask = parentTask;
    }

    @Override
    public void onResponse(Iterable<VectorSchemaRoot> reducedResult) {
        VectorSchemaRoot firstBatch = reducedResult.iterator().hasNext()
            ? reducedResult.iterator().next() : null;
        if (firstBatch == null
            || firstBatch.getVector("__row_id__") == null
            || firstBatch.getVector("shard_id") == null) {
            realListener.onResponse(reducedResult);
            return;
        }

        FetchStageExecution fetchStage = new FetchStageExecution(
            queryId,
            dispatcher,
            allocator,
            shardTargetsSupplier.get(),
            parentTask,
            realListener
        );
        fetchStage.execute(reducedResult);
    }

    @Override
    public void onFailure(Exception e) {
        realListener.onFailure(e);
    }
}
