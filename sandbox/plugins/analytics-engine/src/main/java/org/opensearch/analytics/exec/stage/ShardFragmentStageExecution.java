/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.PendingExecutions;
import org.opensearch.analytics.exec.QueryContext;
import org.opensearch.analytics.exec.StreamingResponseListener;
import org.opensearch.analytics.exec.action.FragmentExecutionArrowResponse;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.planner.dag.ExecutionTarget;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.cluster.service.ClusterService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Leaf stage execution that dispatches fragment work to data-node shards via
 * Arrow streaming, feeding resulting batches into the parent stage's
 * {@link ExchangeSink}.
 *
 * <p>One-shot: constructed, {@link #start()} called once, listener
 * signaled on completion, then discarded.
 *
 * @opensearch.internal
 */
final class ShardFragmentStageExecution extends AbstractStageExecution implements DataProducer {

    private final AtomicInteger inFlight = new AtomicInteger(0);

    private final QueryContext config;
    private final ExchangeSink outputSink;
    private final ClusterService clusterService;
    private final Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder;
    private final AnalyticsSearchTransportService dispatcher;
    private final Map<String, PendingExecutions> pendingPerNode = new ConcurrentHashMap<>();
    private final List<ShardExecutionTarget> resolvedTargets = new ArrayList<>();

    ShardFragmentStageExecution(
        Stage stage,
        QueryContext config,
        ExchangeSink outputSink,
        ClusterService clusterService,
        Function<ShardExecutionTarget, FragmentExecutionRequest> requestBuilder,
        AnalyticsSearchTransportService dispatcher
    ) {
        super(stage);
        this.config = config;
        this.outputSink = outputSink;
        this.clusterService = clusterService;
        this.requestBuilder = requestBuilder;
        this.dispatcher = dispatcher;
    }

    @Override
    public void start() {
        List<ExecutionTarget> resolved = stage.getTargetResolver().resolve(clusterService.state(), null);
        if (resolved.isEmpty()) {
            transitionTo(StageExecution.State.SUCCEEDED);
            return;
        }
        if (transitionTo(StageExecution.State.RUNNING) == false) return;
        inFlight.set(resolved.size());
        for (ExecutionTarget target : resolved) {
            resolvedTargets.add((ShardExecutionTarget) target);
        }
        // QTF: populate context targets BEFORE dispatch (local dispatch is synchronous)
        if (stage.isInjectShardOrdinal()) {
            config.getResolvedShardTargets().addAll(resolvedTargets);
        }
        for (int i = 0; i < resolvedTargets.size(); i++) {
            dispatchShardTask(resolvedTargets.get(i), i);
        }
    }

    private void dispatchShardTask(ShardExecutionTarget target, int shardOrdinal) {
        FragmentExecutionRequest request = requestBuilder.apply(target);
        PendingExecutions pending = pendingFor(target);
        dispatcher.dispatchFragmentStreaming(request, target.node(), responseListener(shardOrdinal), config.parentTask(), pending);
    }

    private StreamingResponseListener<FragmentExecutionArrowResponse> responseListener(int shardOrdinal) {
        return new StreamingResponseListener<>() {
            @Override
            public void onStreamResponse(FragmentExecutionArrowResponse response, boolean isLast) {
                if (isDone()) {
                    VectorSchemaRoot root = response.getRoot();
                    if (root != null) {
                        root.close();
                    }
                    return;
                }

                VectorSchemaRoot vsr = response.getRoot();
                try {
                    if (stage.isInjectShardOrdinal()) {
                        vsr = injectShardId(vsr, shardOrdinal);
                    }
                    outputSink.feed(vsr);
                } catch (Exception e) {
                    captureFailure(new RuntimeException("Stage " + stage.getStageId() + " sink feed failed", e));
                    metrics.incrementTasksFailed();
                    onShardTerminated();
                    return;
                }
                metrics.addRowsProcessed(vsr.getRowCount());

                if (isLast) {
                    metrics.incrementTasksCompleted();
                    onShardTerminated();
                }
            }

            @Override
            public void onFailure(Exception e) {
                captureFailure(new RuntimeException("Stage " + stage.getStageId() + " failed", e));
                metrics.incrementTasksFailed();
                onShardTerminated();
            }
        };
    }

    private void onShardTerminated() {
        if (inFlight.decrementAndGet() == 0) {
            Exception captured = getFailure();
            transitionTo(captured != null ? StageExecution.State.FAILED : StageExecution.State.SUCCEEDED);
        }
    }

    @Override
    public void cancel(String reason) {
        if (transitionTo(StageExecution.State.CANCELLED) == false) return;
        org.opensearch.tasks.Task parentTask = config.parentTask();
        if (parentTask instanceof org.opensearch.tasks.CancellableTask ct && ct.isCancelled() == false) {
            ct.cancel(reason);
        }
    }

    @Override
    public ExchangeSource outputSource() {
        if (outputSink instanceof ExchangeSource source) {
            return source;
        }
        throw new UnsupportedOperationException("outputSink does not implement ExchangeSource");
    }

    /** Returns the ordered list of resolved shard targets. */
    public List<ShardExecutionTarget> getResolvedTargets() {
        return Collections.unmodifiableList(resolvedTargets);
    }

    private boolean isDone() {
        StageExecution.State s = getState();
        return s == StageExecution.State.SUCCEEDED || s == StageExecution.State.FAILED || s == StageExecution.State.CANCELLED;
    }

    private PendingExecutions pendingFor(ShardExecutionTarget target) {
        return pendingPerNode.computeIfAbsent(target.node().getId(), n -> new PendingExecutions(config.maxConcurrentShardRequests()));
    }

    private static VectorSchemaRoot injectShardId(VectorSchemaRoot batch, int shardId) {
        org.apache.arrow.vector.IntVector shardIdVector = new org.apache.arrow.vector.IntVector(
            "shard_id",
            batch.getFieldVectors().get(0).getAllocator()
        );
        shardIdVector.allocateNew(batch.getRowCount());
        for (int i = 0; i < batch.getRowCount(); i++) {
            shardIdVector.set(i, shardId);
        }
        shardIdVector.setValueCount(batch.getRowCount());

        List<org.apache.arrow.vector.FieldVector> vectors = new ArrayList<>(batch.getFieldVectors());
        vectors.add(shardIdVector);
        return new VectorSchemaRoot(vectors);
    }
}
