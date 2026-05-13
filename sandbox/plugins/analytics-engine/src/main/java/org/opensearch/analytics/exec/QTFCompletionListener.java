/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.action.FragmentExecutionRequest;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.planner.dag.ShardExecutionTarget;
import org.opensearch.core.action.ActionListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QTF completion listener: intercepts the reduced query-phase result,
 * builds a position map, dispatches fetch requests per shard, and
 * assembles the final globally-sorted output.
 *
 * <p>Wraps the real completion listener — the caller sees the final
 * assembled result as if the query executed in one shot.
 */
public class QTFCompletionListener implements ActionListener<Iterable<VectorSchemaRoot>> {

    private static final Logger logger = LogManager.getLogger(QTFCompletionListener.class);

    private final ActionListener<Iterable<VectorSchemaRoot>> realListener;
    private final String queryId;
    private final AnalyticsSearchTransportService dispatcher;
    private final BufferAllocator allocator;
    private final java.util.function.Supplier<List<ShardExecutionTarget>> shardTargetsSupplier;
    private final org.opensearch.analytics.exec.task.AnalyticsQueryTask parentTask;

    public QTFCompletionListener(
        ActionListener<Iterable<VectorSchemaRoot>> realListener,
        String queryId,
        AnalyticsSearchTransportService dispatcher,
        BufferAllocator allocator,
        java.util.function.Supplier<List<ShardExecutionTarget>> shardTargetsSupplier,
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
        try {
            // Check if this is actually a QTF query (has __row_id__ + shard_id in result)
            VectorSchemaRoot firstBatch = reducedResult.iterator().hasNext()
                ? reducedResult.iterator().next() : null;
            if (firstBatch == null
                || firstBatch.getVector("__row_id__") == null
                || firstBatch.getVector("shard_id") == null) {
                // Not a QTF query — pass through unchanged
                realListener.onResponse(reducedResult);
                return;
            }

            // Derive fetch columns from reduced result schema (all columns except __row_id__ and shard_id)
            String[] fetchColumns = firstBatch.getSchema().getFields().stream()
                .map(Field::getName)
                .filter(name -> !"__row_id__".equals(name) && !"shard_id".equals(name))
                .toArray(String[]::new);

            // Phase 2.5: Build position map from reduced output
            PositionMap positionMap = buildPositionMap(reducedResult);
            logger.info("[QTF] Position map built: totalRows={}, shards={}",
                positionMap.totalRows(), positionMap.shardCount());

            // Close the reduced result batches — we've extracted what we need (position map)
            for (VectorSchemaRoot batch : reducedResult) {
                batch.close();
            }

            if (positionMap.totalRows() == 0) {
                realListener.onResponse(List.of());
                return;
            }

            // Phase 3: Dispatch fetch requests per shard
            dispatchFetches(positionMap, fetchColumns);
        } catch (Exception e) {
            realListener.onFailure(e);
        }
    }

    @Override
    public void onFailure(Exception e) {
        realListener.onFailure(e);
    }

    // ── Phase 2.5: Build Position Map ──────────────────────────────────────────

    private PositionMap buildPositionMap(Iterable<VectorSchemaRoot> reducedResult) {
        PositionMap map = new PositionMap();
        int pos = 0;

        for (VectorSchemaRoot batch : reducedResult) {
            // __row_id__ may come as UInt8Vector (Arrow UInt64) or BigIntVector (Int64)
            org.apache.arrow.vector.FieldVector rowIdRaw = batch.getVector("__row_id__");
            IntVector shardIdCol = (IntVector) batch.getVector("shard_id");

            if (rowIdRaw == null || shardIdCol == null) {
                throw new IllegalStateException(
                    "[QTF] Reduced result missing __row_id__ or shard_id columns. "
                    + "Schema: " + batch.getSchema()
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
                    // Generic fallback: read as object and convert
                    rowId = ((Number) rowIdRaw.getObject(i)).longValue();
                }
                map.put(shard, rowId, pos);
                pos++;
            }
        }
        return map;
    }

    // ── Phase 3: Dispatch Fetches ──────────────────────────────────────────────

    private void dispatchFetches(PositionMap positionMap, String[] fetchColumns) {
        List<ShardExecutionTarget> shardTargets = shardTargetsSupplier.get();
        Map<Integer, long[]> fetchPlan = positionMap.getPerShardFetchPlan();

        AtomicInteger remaining = new AtomicInteger(fetchPlan.size());
        List<FetchResult> fetchResults = java.util.Collections.synchronizedList(new ArrayList<>());

        for (Map.Entry<Integer, long[]> entry : fetchPlan.entrySet()) {
            int shardOrdinal = entry.getKey();
            long[] rowIds = entry.getValue();

            if (shardOrdinal >= shardTargets.size()) {
                realListener.onFailure(new IllegalStateException(
                    "[QTF] Shard ordinal " + shardOrdinal + " exceeds target count " + shardTargets.size()
                ));
                return;
            }

            ShardExecutionTarget target = shardTargets.get(shardOrdinal);
            FragmentExecutionRequest fetchReq = FragmentExecutionRequest.fetchMode(
                queryId, target.shardId(), rowIds, fetchColumns
            );

            logger.info("[QTF] Dispatching fetch to shard {} (ordinal={}): {} row_ids",
                target.shardId(), shardOrdinal, rowIds.length);

            dispatcher.dispatchFragment(
                fetchReq,
                target.node(),
                new FetchResponseListener(shardOrdinal, positionMap, fetchResults, remaining),
                parentTask,
                new PendingExecutions(10)
            );
        }
    }

    // ── Phase 4: Assembly ──────────────────────────────────────────────────────

    private class FetchResponseListener implements StreamingResponseListener<FragmentExecutionResponse> {
        private final int shardOrdinal;
        private final PositionMap positionMap;
        private final List<FetchResult> fetchResults;
        private final AtomicInteger remaining;

        FetchResponseListener(
            int shardOrdinal,
            PositionMap positionMap,
            List<FetchResult> fetchResults,
            AtomicInteger remaining
        ) {
            this.shardOrdinal = shardOrdinal;
            this.positionMap = positionMap;
            this.fetchResults = fetchResults;
            this.remaining = remaining;
        }

        @Override
        public void onStreamResponse(FragmentExecutionResponse response, boolean isLast) {
            // TODO: decode response to VectorSchemaRoot and collect
            // For now, collect raw response
            fetchResults.add(new FetchResult(shardOrdinal, response));

            if (isLast && remaining.decrementAndGet() == 0) {
                assembleAndDeliver();
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("[QTF] Fetch failed for shard ordinal={}", shardOrdinal, e);
            realListener.onFailure(e);
        }

        private void assembleAndDeliver() {
            try {
                VectorSchemaRoot assembled = assembleResult(fetchResults, positionMap);
                realListener.onResponse(List.of(assembled));
            } catch (Exception e) {
                realListener.onFailure(e);
            }
        }
    }

    /**
     * Assemble fetched rows into a single result buffer in globally-sorted order.
     * Decodes fetch responses (Arrow IPC), strips __row_id__, and places rows at
     * their correct position using the position map.
     */
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

                    // Lazy-init output on first batch
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

                    // Read __row_id__ from fetch response to place rows at correct positions
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

    // ── Supporting types ───────────────────────────────────────────────────────

    private record FetchResult(int shardOrdinal, FragmentExecutionResponse response) {}

    /**
     * Maps (shard_ordinal, row_id) → position in final output.
     * Also provides grouped row_ids per shard for fetch dispatch.
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
                throw new IllegalStateException(
                    "[QTF] No position for shard=" + shard + " rowId=" + rowId
                );
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
