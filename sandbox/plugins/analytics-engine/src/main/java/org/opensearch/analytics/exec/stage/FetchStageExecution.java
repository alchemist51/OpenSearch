/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.ExchangeSource;
import org.opensearch.analytics.exec.AnalyticsSearchTransportService;
import org.opensearch.analytics.exec.action.FetchByRowIdsRequest;
import org.opensearch.analytics.exec.action.FetchByRowIdsResponse;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.analytics.planner.dag.Stage;
import org.opensearch.analytics.spi.ExchangeSink;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QTF fetch stage execution. Consumes the reduced query-phase output (containing
 * __row_id__ + shard_id + sort columns), builds a position map, dispatches
 * fetch-by-row-id requests to data nodes, and assembles the final globally-sorted
 * result with materialized columns.
 *
 * <p>Lifecycle:
 * <ol>
 *   <li>{@link #execute(Iterable)} — called with the reduced result from the parent stage</li>
 *   <li>Builds position map from __row_id__ + shard_id</li>
 *   <li>Dispatches per-shard fetch requests via transport</li>
 *   <li>Assembles responses into globally-sorted output</li>
 *   <li>Delivers result to the completion listener</li>
 * </ol>
 *
 * @opensearch.internal
 */
public class FetchStageExecution {

    private static final Logger logger = LogManager.getLogger(FetchStageExecution.class);

    private final String queryId;
    private final AnalyticsSearchTransportService dispatcher;
    private final BufferAllocator allocator;
    private final List<ShardExecutionTarget> shardTargets;
    private final Task parentTask;
    private final ActionListener<Iterable<VectorSchemaRoot>> completionListener;

    public FetchStageExecution(
        String queryId,
        AnalyticsSearchTransportService dispatcher,
        BufferAllocator allocator,
        List<ShardExecutionTarget> shardTargets,
        Task parentTask,
        ActionListener<Iterable<VectorSchemaRoot>> completionListener
    ) {
        this.queryId = queryId;
        this.dispatcher = dispatcher;
        this.allocator = allocator;
        this.shardTargets = shardTargets;
        this.parentTask = parentTask;
        this.completionListener = completionListener;
    }

    /**
     * Execute the fetch stage with the reduced query-phase result.
     * This is the main entry point — drives position map → dispatch → assembly.
     */
    public void execute(Iterable<VectorSchemaRoot> reducedResult) {
        try {
            VectorSchemaRoot firstBatch = reducedResult.iterator().hasNext()
                ? reducedResult.iterator().next() : null;
            if (firstBatch == null
                || firstBatch.getVector("__row_id__") == null
                || firstBatch.getVector("shard_id") == null) {
                completionListener.onResponse(reducedResult);
                return;
            }

            String[] fetchColumns = firstBatch.getSchema().getFields().stream()
                .map(Field::getName)
                .filter(name -> !"__row_id__".equals(name) && !"shard_id".equals(name))
                .toArray(String[]::new);

            PositionMap positionMap = buildPositionMap(reducedResult);
            logger.info("[QTF] Position map built: totalRows={}, shards={}",
                positionMap.totalRows(), positionMap.shardCount());

            for (VectorSchemaRoot batch : reducedResult) {
                batch.close();
            }

            if (positionMap.totalRows() == 0) {
                completionListener.onResponse(List.of());
                return;
            }

            dispatchFetches(positionMap, fetchColumns);
        } catch (Exception e) {
            completionListener.onFailure(e);
        }
    }

    // ── Position Map ─────────────────────────────────────────────────────────────

    private PositionMap buildPositionMap(Iterable<VectorSchemaRoot> reducedResult) {
        PositionMap map = new PositionMap();
        int pos = 0;

        for (VectorSchemaRoot batch : reducedResult) {
            FieldVector rowIdRaw = batch.getVector("__row_id__");
            IntVector shardIdCol = (IntVector) batch.getVector("shard_id");

            if (rowIdRaw == null || shardIdCol == null) {
                throw new IllegalStateException(
                    "[QTF] Reduced result missing __row_id__ or shard_id. Schema: " + batch.getSchema()
                );
            }

            for (int i = 0; i < batch.getRowCount(); i++) {
                int shard = shardIdCol.get(i);
                long rowId;
                if (rowIdRaw instanceof BigIntVector bigInt) {
                    rowId = bigInt.get(i);
                } else if (rowIdRaw instanceof org.apache.arrow.vector.UInt8Vector uint8) {
                    rowId = uint8.get(i);
                } else {
                    rowId = ((Number) rowIdRaw.getObject(i)).longValue();
                }
                map.put(shard, rowId, pos);
                pos++;
            }
        }
        return map;
    }

    // ── Fetch Dispatch ───────────────────────────────────────────────────────────

    private void dispatchFetches(PositionMap positionMap, String[] fetchColumns) {
        Map<Integer, long[]> fetchPlan = positionMap.getPerShardFetchPlan();

        AtomicInteger remaining = new AtomicInteger(fetchPlan.size());
        List<FetchResult> fetchResults = java.util.Collections.synchronizedList(new ArrayList<>());

        for (Map.Entry<Integer, long[]> entry : fetchPlan.entrySet()) {
            int shardOrdinal = entry.getKey();
            long[] rowIds = entry.getValue();

            if (shardOrdinal >= shardTargets.size()) {
                completionListener.onFailure(new IllegalStateException(
                    "[QTF] Shard ordinal " + shardOrdinal + " exceeds target count " + shardTargets.size()
                ));
                return;
            }

            ShardExecutionTarget target = shardTargets.get(shardOrdinal);
            FetchByRowIdsRequest fetchReq = new FetchByRowIdsRequest(
                queryId, target.shardId(), rowIds, fetchColumns
            );

            logger.info("[QTF] Dispatching fetch to shard {} (ordinal={}): {} row_ids",
                target.shardId(), shardOrdinal, rowIds.length);

            dispatcher.dispatchFetch(
                fetchReq,
                target.node(),
                new FetchResponseListener(shardOrdinal, positionMap, fetchResults, remaining),
                parentTask
            );
        }
    }

    // ── Assembly ─────────────────────────────────────────────────────────────────

    private class FetchResponseListener implements ActionListener<FetchByRowIdsResponse> {
        private final int shardOrdinal;
        private final PositionMap positionMap;
        private final List<FetchResult> fetchResults;
        private final AtomicInteger remaining;

        FetchResponseListener(int shardOrdinal, PositionMap positionMap, List<FetchResult> fetchResults, AtomicInteger remaining) {
            this.shardOrdinal = shardOrdinal;
            this.positionMap = positionMap;
            this.fetchResults = fetchResults;
            this.remaining = remaining;
        }

        @Override
        public void onResponse(FetchByRowIdsResponse response) {
            fetchResults.add(new FetchResult(shardOrdinal, response));
            if (remaining.decrementAndGet() == 0) {
                assembleAndDeliver();
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("[QTF] Fetch failed for shard ordinal={}", shardOrdinal, e);
            completionListener.onFailure(e);
        }

        private void assembleAndDeliver() {
            try {
                VectorSchemaRoot assembled = assembleResult(fetchResults, positionMap);
                completionListener.onResponse(List.of(assembled));
            } catch (Exception e) {
                completionListener.onFailure(e);
            }
        }
    }

    private VectorSchemaRoot assembleResult(List<FetchResult> fetchResults, PositionMap positionMap) {
        logger.info("[QTF] Assembling {} fetch results into {} positions",
            fetchResults.size(), positionMap.totalRows());

        int totalRows = positionMap.totalRows();
        VectorSchemaRoot output = null;
        List<Field> outputFields = null;

        for (FetchResult fr : fetchResults) {
            byte[] ipc = fr.response().getIpcPayload();
            if (ipc == null || ipc.length == 0) continue;
            int shardOrdinal = fr.shardOrdinal();

            try (var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(
                    new java.io.ByteArrayInputStream(ipc), allocator)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot batch = reader.getVectorSchemaRoot();
                    int batchRows = batch.getRowCount();

                    if (output == null) {
                        outputFields = batch.getSchema().getFields().stream()
                            .filter(f -> !"__row_id__".equals(f.getName()))
                            .toList();
                        output = VectorSchemaRoot.create(new Schema(outputFields), allocator);
                        output.allocateNew();
                        output.setRowCount(totalRows);
                        for (FieldVector v : output.getFieldVectors()) {
                            v.setValueCount(totalRows);
                        }
                    }

                    FieldVector rowIdRaw = batch.getVector("__row_id__");
                    for (int i = 0; i < batchRows; i++) {
                        long rowId;
                        if (rowIdRaw instanceof BigIntVector bigInt) {
                            rowId = bigInt.get(i);
                        } else if (rowIdRaw instanceof org.apache.arrow.vector.UInt8Vector uint8) {
                            rowId = uint8.get(i);
                        } else {
                            rowId = ((Number) rowIdRaw.getObject(i)).longValue();
                        }

                        int destPos = positionMap.getPosition(shardOrdinal, rowId);
                        for (Field f : outputFields) {
                            FieldVector src = batch.getVector(f.getName());
                            FieldVector dst = output.getVector(f.getName());
                            dst.copyFrom(i, destPos, src);
                        }
                    }
                }
            } catch (Exception e) {
                if (output != null) output.close();
                throw new RuntimeException("[QTF] Failed to decode fetch response", e);
            }
        }

        if (output == null) {
            return VectorSchemaRoot.create(new Schema(List.of()), allocator);
        }
        return output;
    }

    // ── Supporting types ─────────────────────────────────────────────────────────

    private record FetchResult(int shardOrdinal, FetchByRowIdsResponse response) {}

    /**
     * Maps (shard_ordinal, row_id) → position in final output.
     */
    static class PositionMap {
        private final Map<Long, Integer> positionLookup = new HashMap<>();
        private final Map<Integer, List<Long>> perShardRowIds = new HashMap<>();
        private int totalRows = 0;

        void put(int shard, long rowId, int position) {
            positionLookup.put(encode(shard, rowId), position);
            perShardRowIds.computeIfAbsent(shard, k -> new ArrayList<>()).add(rowId);
            totalRows++;
        }

        int getPosition(int shard, long rowId) {
            Integer pos = positionLookup.get(encode(shard, rowId));
            if (pos == null) {
                throw new IllegalStateException("[QTF] No position for shard=" + shard + " rowId=" + rowId);
            }
            return pos;
        }

        Map<Integer, long[]> getPerShardFetchPlan() {
            return perShardRowIds.entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().stream().mapToLong(Long::longValue).toArray()
                ));
        }

        int totalRows() { return totalRows; }
        int shardCount() { return perShardRowIds.size(); }

        private static long encode(int shard, long rowId) {
            return ((long) shard << 40) | rowId;
        }
    }
}
