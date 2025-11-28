/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.datafusion.search.*;
import org.opensearch.datafusion.search.cache.CacheManager;
import org.opensearch.datafusion.search.cache.CacheUtils;
import org.opensearch.index.engine.*;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.composite.CompositeDataFormatWriter;
import org.opensearch.index.mapper.*;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.internal.ReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.vectorized.execution.search.DataFormat;
import org.opensearch.search.query.GenericQueryPhaseSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class DatafusionEngine extends SearchExecEngine<DatafusionContext, DatafusionSearcher,
    DatafusionReaderManager, DatafusionQuery> {

    private static final Logger logger = LogManager.getLogger(DatafusionEngine.class);

    private DataFormat dataFormat;
    private DatafusionReaderManager datafusionReaderManager;
    private DataFusionService datafusionService;
    private CacheManager cacheManager;

    // Helper class to track timing across async operations in a thread-safe manner
    private static class AsyncTimingContext {
        private final AtomicLong phaseStartTime;
        private final AtomicLong searchStartTime;
        private final AtomicLong searchEndTime;
        private final AtomicLong collectionStartTime;
        private final AtomicLong collectionEndTime;
        private final AtomicLong batchCounter;

        public AsyncTimingContext() {
            this.phaseStartTime = new AtomicLong(System.nanoTime());
            this.searchStartTime = new AtomicLong(0);
            this.searchEndTime = new AtomicLong(0);
            this.collectionStartTime = new AtomicLong(0);
            this.collectionEndTime = new AtomicLong(0);
            this.batchCounter = new AtomicLong(0);
        }

        public void markSearchStart() {
            searchStartTime.set(System.nanoTime());
        }

        public void markSearchEnd() {
            searchEndTime.set(System.nanoTime());
        }

        public void markCollectionStart() {
            collectionStartTime.set(System.nanoTime());
        }

        public void markCollectionEnd() {
            collectionEndTime.set(System.nanoTime());
        }

        public long incrementBatch() {
            return batchCounter.incrementAndGet();
        }

        public long getPhaseStartTime() {
            return phaseStartTime.get();
        }

        public double getSearchDurationMs() {
            long start = searchStartTime.get();
            long end = searchEndTime.get();
            return (start > 0 && end > 0) ? (end - start) / 1_000_000.0 : 0;
        }

        public double getCollectionDurationMs() {
            long start = collectionStartTime.get();
            long end = collectionEndTime.get();
            return (start > 0 && end > 0) ? (end - start) / 1_000_000.0 : 0;
        }

        public double getTotalDurationMs() {
            return (System.nanoTime() - phaseStartTime.get()) / 1_000_000.0;
        }

        public long getBatchCount() {
            return batchCounter.get();
        }
    }

    public DatafusionEngine(DataFormat dataFormat, Collection<FileMetadata> formatCatalogSnapshot, DataFusionService dataFusionService, ShardPath shardPath) throws IOException {
        this.dataFormat = dataFormat;

        this.datafusionReaderManager = new DatafusionReaderManager(shardPath.getDataPath().toString(), formatCatalogSnapshot, dataFormat.getName());
        this.datafusionService = dataFusionService;
        this.cacheManager = datafusionService.getCacheManager();
        if(this.cacheManager!=null) {
            datafusionReaderManager.setOnFilesAdded(files -> {
                // Handle new files added during refresh
                cacheManager.addFilesToCacheManager(files);
            });
        }
    }

    @Override
    public DatafusionContext createContext(ReaderContext readerContext, ShardSearchRequest request, SearchShardTarget searchShardTarget, SearchShardTask task, BigArrays bigArrays, SearchContext originalContext) throws IOException {
        DatafusionContext datafusionContext = new DatafusionContext(readerContext, request, searchShardTarget, task, this, bigArrays, originalContext);
        // Parse source
        datafusionContext.datafusionQuery(new DatafusionQuery(request.shardId().getIndexName(), request.source().queryPlanIR(), new ArrayList<>()));
        return datafusionContext;
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        return acquireSearcherSupplier(wrapper, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public EngineSearcherSupplier<DatafusionSearcher> acquireSearcherSupplier(Function<DatafusionSearcher, DatafusionSearcher> wrapper, Engine.SearcherScope scope) throws EngineException {
        // TODO : wrapper is ignored
        EngineSearcherSupplier<DatafusionSearcher> searcher = null;
        // TODO : refcount needs to be revisited - add proper tests for exception etc
        try {
            DatafusionReader reader = datafusionReaderManager.acquire();
            searcher = new DatafusionSearcherSupplier(null) {
                @Override
                protected DatafusionSearcher acquireSearcherInternal(String source) {
                    return new DatafusionSearcher(source, reader,
                        () -> {});

                }

                @Override
                protected void doClose() {
                    try {
                        datafusionReaderManager.release(reader);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        } catch (Exception ex) {
            logger.error("Failed to acquire searcher {}", ex.toString(), ex);
            // TODO
        }
        return searcher;
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source) throws EngineException {
        return acquireSearcher(source, Engine.SearcherScope.EXTERNAL);
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope) throws EngineException {
        return acquireSearcher(source, scope, Function.identity());
    }

    @Override
    public DatafusionSearcher acquireSearcher(String source, Engine.SearcherScope scope, Function<DatafusionSearcher, DatafusionSearcher> wrapper) throws EngineException {
        DatafusionSearcherSupplier releasable = null;
        try {
            DatafusionSearcherSupplier searcherSupplier = releasable = (DatafusionSearcherSupplier) acquireSearcherSupplier(wrapper, scope);
            DatafusionSearcher searcher = searcherSupplier.acquireSearcher(source);
            releasable = null;

            return new DatafusionSearcher(
                source,
                searcher.getReader(),
                () -> Releasables.close(searcher, searcherSupplier)
            );
        } finally {
            Releasables.close(releasable);
        }
    }

    @Override
    public DatafusionReaderManager getReferenceManager(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public CatalogSnapshotAwareRefreshListener getRefreshListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public FileDeletionListener getFileDeletionListener(Engine.SearcherScope scope) {
        return datafusionReaderManager;
    }

    @Override
    public boolean assertSearcherIsWarmedUp(String source, Engine.SearcherScope scope) {
        return false;
    }


    @Override
    public Map<String, Object[]> executeQueryPhase(DatafusionContext context) {
        long startTime = System.nanoTime();
        logger.info("[QueryPhase] Starting query phase for shard: {}", context.indexShard().shardId());

        Map<String, Object[]> finalRes = new HashMap<>();
        List<Long> rowIdResult = new ArrayList<>();
        RootAllocator allocator = null;
        RecordBatchStream stream = null;

        long searchStartTime = 0;
        long searchEndTime = 0;
        long collectionStartTime = 0;
        long collectionEndTime = 0;

        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();

            searchStartTime = System.nanoTime();
            long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
            searchEndTime = System.nanoTime();
            logger.info("[QueryPhase] Search execution took: {} ms", (searchEndTime - searchStartTime) / 1_000_000.0);

            allocator = new RootAllocator(Long.MAX_VALUE);
            stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer() , allocator);

            SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                @Override
                public void collect(RecordBatchStream value) {
                    VectorSchemaRoot root = value.getVectorSchemaRoot();
                    for (Field field : root.getSchema().getFields()) {
                        String fieldName = field.getName();
                        FieldVector fieldVector = root.getVector(fieldName);
                        Object[] fieldValues = new Object[fieldVector.getValueCount()];
                        if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                            FieldVector rowIdVector = root.getVector(fieldName);
                            for(int i=0; i<fieldVector.getValueCount(); i++) {
                                rowIdResult.add((long) rowIdVector.getObject(i));
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        else {
                            for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                fieldValues[i] = fieldVector.getObject(i);
                            }
                        }
                        finalRes.put(fieldName, fieldValues);
                    }
                }
            };

            collectionStartTime = System.nanoTime();
            int batchCount = 0;
            while (stream.loadNextBatch().join()) {
                long batchStartTime = System.nanoTime();
                collector.collect(stream);
                batchCount++;
                logger.debug("[QueryPhase] Batch {} collection took: {} ms", batchCount, (System.nanoTime() - batchStartTime) / 1_000_000.0);
            }
            collectionEndTime = System.nanoTime();
            logger.info("[QueryPhase] Total batches processed: {}, Collection took: {} ms", batchCount, (collectionEndTime - collectionStartTime) / 1_000_000.0);

        } catch (Exception exception) {
            logger.error("Failed to execute Substrait query plan", exception);
            throw new RuntimeException(exception);
        } finally {
            try {
                if (stream != null) {
                    stream.close();
                }
                if (allocator != null) {
                    allocator.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
//            logger.info("[QueryPhase] Query phase completed for shard: {} | Total time: {} ms | Search: {} ms | Collection: {} ms | Result size: {}",
//                context.indexShard().shardId(),
//                totalTime / 1_000_000.0,
//                searchEndTime > 0 ? (searchEndTime - searchStartTime) / 1_000_000.0 : 0,
//                collectionEndTime > 0 ? (collectionEndTime - collectionStartTime) / 1_000_000.0 : 0,
//                rowIdResult.size());
        }

        context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(), TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(), Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
        return finalRes;
    }

    @Override
    public void executeQueryPhaseAsync(DatafusionContext context, Executor executor, ActionListener<Map<String, Object[]>> listener) {
        final AsyncTimingContext timingContext = new AsyncTimingContext();
        //logger.info("[QueryPhaseAsync] Starting async query phase for shard: {}", context.indexShard().shardId());

        try {
            DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
            timingContext.markSearchStart();

            datafusionSearcher.searchAsync(context.getDatafusionQuery(), datafusionService.getRuntimePointer()).whenCompleteAsync((streamPointer, error)-> {
                timingContext.markSearchEnd();
                //logger.info("[QueryPhaseAsync] Async search execution took: {} ms", timingContext.getSearchDurationMs());

                if(streamPointer == null) {
                    logger.error("[QueryPhaseAsync] Query phase failed for shard: {} | Total time: {} ms",
                        context.indexShard().shardId(), timingContext.getTotalDurationMs());
                    listener.onFailure(new RuntimeException("Search returned null stream pointer", error));
                    return;
                }

                final Map<String, Object[]> finalRes = new HashMap<>();
                final List<Long> rowIdResult = new ArrayList<>();

                RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
                RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer() , allocator);

                SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
                    @Override
                    public void collect(RecordBatchStream value) {
                        VectorSchemaRoot root = value.getVectorSchemaRoot();
                        for (Field field : root.getSchema().getFields()) {
                            String fieldName = field.getName();
                            FieldVector fieldVector = root.getVector(fieldName);
                            Object[] fieldValues = new Object[fieldVector.getValueCount()];
                            if (fieldName.equals(CompositeDataFormatWriter.ROW_ID)) {
                                FieldVector rowIdVector = root.getVector(fieldName);
                                for(int i=0; i<fieldVector.getValueCount(); i++) {
                                    rowIdResult.add((long) rowIdVector.getObject(i));
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            else {
                                for (int i = 0; i < fieldVector.getValueCount(); i++) {
                                    fieldValues[i] = fieldVector.getObject(i);
                                }
                            }
                            finalRes.put(fieldName, fieldValues);
                        }
                    }
                };

                timingContext.markCollectionStart();

                loadNextBatchAsync(stream, executor, collector, finalRes, allocator,
                    ActionListener.wrap(
                        response -> {
                            timingContext.markCollectionEnd();
//                            logger.info("[QueryPhaseAsync] Query phase completed for shard: {} | Total time: {} ms | Search: {} ms | Collection: {} ms | Batches: {} | Result size: {}",
//                                context.indexShard().shardId(),
//                                timingContext.getTotalDurationMs(),
//                                timingContext.getSearchDurationMs(),
//                                timingContext.getCollectionDurationMs(),
//                                timingContext.getBatchCount(),
//                                rowIdResult.size());
                            listener.onResponse(response);
                        },
                        failure -> {
                            logger.error("[QueryPhaseAsync] Query phase failed for shard: {} | Total time: {} ms",
                                context.indexShard().shardId(), timingContext.getTotalDurationMs());
                            listener.onFailure(failure);
                        }
                    ),
                    context, rowIdResult, timingContext);
            }, executor);

        } catch (Exception exception) {
            logger.error("[QueryPhaseAsync] Failed to execute Substrait query plan | Time: {} ms",
                timingContext.getTotalDurationMs(), exception);
            listener.onFailure(exception);
        }
    }

    private void loadNextBatchAsync(
        RecordBatchStream stream,
        Executor executor,
        SearchResultsCollector<RecordBatchStream> collector,
        Map<String, Object[]> finalRes,
        RootAllocator allocator,
        ActionListener<Map<String, Object[]>> listener,
        DatafusionContext context,
        List<Long> rowIdResult,
        AsyncTimingContext timingContext
    ) {
        final long batchStartTime = System.nanoTime();

        stream.loadNextBatch().whenCompleteAsync((hasMore, error) -> {
            final long batchLoadEndTime = System.nanoTime();
            final double batchLoadTimeMs = (batchLoadEndTime - batchStartTime) / 1_000_000.0;

            if (error != null) {
                logger.error("[LoadNextBatch] Error loading batch {} after {} ms",
                    timingContext.getBatchCount(), batchLoadTimeMs, error);
                cleanup(stream, allocator);
                listener.onFailure(new RuntimeException("Error loading batch", error));
                return;
            }

            if (hasMore) {
                try {
                    final long collectStartTime = System.nanoTime();
                    collector.collect(stream);
                    final long collectEndTime = System.nanoTime();
                    final double collectTimeMs = (collectEndTime - collectStartTime) / 1_000_000.0;
                    final long currentBatch = timingContext.incrementBatch();

                    logger.debug("[LoadNextBatch] Batch {} completed | Load: {} ms | Collect: {} ms | Total: {} ms",
                        currentBatch,
                        batchLoadTimeMs,
                        collectTimeMs,
                        (collectEndTime - batchStartTime) / 1_000_000.0);

                    // Recursively load next batch
                    loadNextBatchAsync(stream, executor, collector, finalRes, allocator, listener, context, rowIdResult, timingContext);
                } catch (Exception e) {
                    logger.error("[LoadNextBatch] Error collecting batch {} after {} ms",
                        timingContext.getBatchCount(), (System.nanoTime() - batchStartTime) / 1_000_000.0, e);
                    cleanup(stream, allocator);
                    listener.onFailure(e);
                }
            } else {
//                logger.info("[LoadNextBatch] All batches loaded | Total batches: {} | Final batch load time: {} ms",
//                    timingContext.getBatchCount(), batchLoadTimeMs);
                cleanup(stream, allocator);

                context.queryResult().topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(rowIdResult.size(),
                    TotalHits.Relation.EQUAL_TO), rowIdResult.stream().map(d-> new ScoreDoc(d.intValue(),
                    Float.NaN, context.indexShard().shardId().getId())).toList().toArray(ScoreDoc[]::new)) , Float.NaN), new DocValueFormat[0]);
                listener.onResponse(finalRes);
            }
        }, executor);
    }

    private void cleanup(RecordBatchStream stream, RootAllocator allocator) {
        long cleanupStartTime = System.nanoTime();
        try {
            if (stream != null) stream.close();
            if (allocator != null) allocator.close();
            logger.debug("[Cleanup] Cleanup completed in {} ms", (System.nanoTime() - cleanupStartTime) / 1_000_000.0);
        } catch (Exception e) {
            logger.error("[Cleanup] Cleanup error after {} ms", (System.nanoTime() - cleanupStartTime) / 1_000_000.0, e);
        }
    }


    /**
     * Executes fetch phase, DataFusion query should contain projections for fields
     * @param context DataFusion context
     * @throws IOException
     */
    @Override
    public void executeFetchPhase(DatafusionContext context) throws IOException {
        long startTime = System.nanoTime();
//        logger.info("[FetchPhase] Starting fetch phase for shard: {} | Docs to load: {}",
//            context.indexShard().shardId(), context.docIdsToLoad().length);

        List<Long> rowIds = Arrays.stream(context.docIdsToLoad()).mapToObj(Long::valueOf).toList();
        if (rowIds.isEmpty()) {
            // no individual hits to process, so we shortcut
            context.fetchResult()
                .hits(new SearchHits(new SearchHit[0], context.queryResult().getTotalHits(), context.queryResult().getMaxScore()));
//            logger.info("[FetchPhase] No docs to fetch for shard: {} | Time: {} ms",
//                context.indexShard().shardId(), (System.nanoTime() - startTime) / 1_000_000.0);
            return;
        }

        long preprocessStartTime = System.nanoTime();
        // preprocess
        context.getDatafusionQuery().setFetchPhaseContext(rowIds);
        List<String> projections = new ArrayList<>(List.of(context.request().source().fetchSource().includes()));
        projections.add(CompositeDataFormatWriter.ROW_ID);
        context.getDatafusionQuery().setProjections(projections);
        long preprocessEndTime = System.nanoTime();
       // logger.debug("[FetchPhase] Preprocessing took: {} ms", (preprocessEndTime - preprocessStartTime) / 1_000_000.0);

        long searchStartTime = System.nanoTime();
        DatafusionSearcher datafusionSearcher = context.getEngineSearcher();
        long streamPointer = datafusionSearcher.search(context.getDatafusionQuery(), datafusionService.getRuntimePointer());
        long searchEndTime = System.nanoTime();
        //logger.info("[FetchPhase] Search execution took: {} ms", (searchEndTime - searchStartTime) / 1_000_000.0);

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        RecordBatchStream stream = new RecordBatchStream(streamPointer, datafusionService.getRuntimePointer() , allocator);

        Map<Long, Integer> rowIdToIndex = new HashMap<>();
        for (int idx = 0; idx < rowIds.size(); idx++) {
            rowIdToIndex.put(rowIds.get(idx), idx);
        }

        MapperService mapperService = context.mapperService();
        MappingLookup mappingLookup = mapperService.documentMapper().mappers();

        long collectionStartTime = System.nanoTime();
        SearchResultsCollector<RecordBatchStream> collector = recordBatchStream -> {
            List<BytesReference> byteRefs = new ArrayList<>();
            SearchHit[] hits = new SearchHit[rowIds.size()];
            int totalHits = 0;
            int batchCount = 0;

            while (recordBatchStream.loadNextBatch().join()) {
                long batchStartTime = System.nanoTime();
                batchCount++;

                VectorSchemaRoot vectorSchemaRoot = recordBatchStream.getVectorSchemaRoot();
                List<FieldVector> fieldVectorList = vectorSchemaRoot.getFieldVectors();
                int rowCount = vectorSchemaRoot.getRowCount();

                for (int i = 0; i < rowCount; i++) {
                    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
                    String _id = "_id";
                    Long row_id = null;

                    try {
                        for (FieldVector valueVectors : fieldVectorList) {
                            if (valueVectors.getName().equals(CompositeDataFormatWriter.ROW_ID)) {
                                row_id = (long) valueVectors.getObject(i);
                                continue;
                            }
                            Mapper mapper = mappingLookup.getMapper(valueVectors.getName());
                            DerivedFieldGenerator derivedFieldGenerator = mapper.derivedFieldGenerator();

                            Object value = valueVectors.getObject(i);
                            if(valueVectors instanceof ViewVarCharVector) {
                                BytesRef bytesRef = new BytesRef(((ViewVarCharVector) valueVectors).get(i));
                                derivedFieldGenerator.generate(builder, List.of(bytesRef));
                            } else {
                                derivedFieldGenerator.generate(builder, List.of(value));
                            }
                            if (valueVectors.getName().equals(IdFieldMapper.NAME)) {
                                BytesRef idRef = new BytesArray((byte[]) value).toBytesRef();
                                _id = Uid.decodeId(idRef.bytes, idRef.offset, idRef.length);
                            }
                        }
                    } catch (Exception e) {
                        logger.error("[FetchPhase] Failed to derive source for doc id [{}]", i, e);
                        throw new OpenSearchException("Failed to derive source for doc id [" + i + "]", e);
                    } finally {
                        builder.endObject();
                    }
                    assert row_id != null || rowIds.get(i) != null;
                    assert _id != null;
                    BytesReference document = BytesReference.bytes(builder);
                    byteRefs.add(document);
                    SearchHit hit = new SearchHit(Math.toIntExact(rowIds.get(i)), _id, emptyMap(), emptyMap());
                    hit.sourceRef(document);
                    FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(hit, null, Math.toIntExact(rowIds.get(i)), new SourceLookup());
                    hitContext.sourceLookup().setSource(document);
                    int index = rowIdToIndex.get(row_id);
                    hits[index] = hit;
                    totalHits++;
                }

                long batchEndTime = System.nanoTime();
//                logger.debug("[FetchPhase] Batch {} processed | Rows: {} | Time: {} ms",
//                    batchCount, rowCount, (batchEndTime - batchStartTime) / 1_000_000.0);
            }

            long collectionEndTime = System.nanoTime();
//            logger.info("[FetchPhase] Collection completed | Total batches: {} | Total hits: {} | Time: {} ms",
//                batchCount, totalHits, (collectionEndTime - collectionStartTime) / 1_000_000.0);

            context.fetchResult().hits(new SearchHits(hits, new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), context.queryResult().getMaxScore()));
        };

        try {
            collector.collect(stream);
        } catch (IOException exception) {
            logger.error("[FetchPhase] Failed to perform fetch phase after {} ms",
                (System.nanoTime() - startTime) / 1_000_000.0, exception);
            throw new RuntimeException(exception);
        } finally {
            long cleanupStartTime = System.nanoTime();
            try {
                stream.close();
                allocator.close();
                logger.debug("[FetchPhase] Cleanup completed in {} ms", (System.nanoTime() - cleanupStartTime) / 1_000_000.0);
            } catch (Exception e) {
                logger.error("[FetchPhase] Failed to close stream after {} ms",
                    (System.nanoTime() - cleanupStartTime) / 1_000_000.0, e);
                throw new RuntimeException(e);
            }

            long endTime = System.nanoTime();
            long totalTime = endTime - startTime;
            logger.info("[FetchPhase] Fetch phase completed for shard: {} | Total time: {} ms | Preprocess: {} ms | Search: {} ms | Collection: {} ms | Hits: {}",
                context.indexShard().shardId(),
                totalTime / 1_000_000.0,
                (preprocessEndTime - preprocessStartTime) / 1_000_000.0,
                (searchEndTime - searchStartTime) / 1_000_000.0,
                (System.nanoTime() - collectionStartTime) / 1_000_000.0,
                rowIds.size());
        }
    }
}
