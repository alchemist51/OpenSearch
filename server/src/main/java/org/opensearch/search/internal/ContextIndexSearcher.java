/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.internal;

import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ParquetExec;
import org.apache.arrow.datafusion.RecordBatchStream;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryCache;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.SparseFixedBitSet;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.lucene.util.CombinedBitSet;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchService;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.parquet.ArrowBatchCollector;
import org.opensearch.search.parquet.ArrowFilter;
import org.opensearch.search.parquet.ArrowQueryContext;
import org.opensearch.search.parquet.ParquetExecQueryContext;
import org.opensearch.search.parquet.substrait.SubstraitFilterProvider;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.Timer;
import org.opensearch.search.profile.query.ProfileWeight;
import org.opensearch.search.profile.query.QueryProfiler;
import org.opensearch.search.profile.query.QueryTimingType;
import org.opensearch.search.query.QueryPhase;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.MinAndMax;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import io.substrait.proto.ExtendedExpression;
import org.roaringbitmap.RoaringBitmap;

/**
 * Context-aware extension of {@link IndexSearcher}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ContextIndexSearcher extends IndexSearcher implements Releasable {

    private static final Logger logger = LogManager.getLogger(ContextIndexSearcher.class);
    /**
     * The interval at which we check for search cancellation when we cannot use
     * a {@link CancellableBulkScorer}. See {@link #intersectScorerAndBitSet}.
     */

    private static final int CHECK_CANCELLED_SCORER_INTERVAL = 1 << 11;

    private AggregatedDfs aggregatedDfs;
    private QueryProfiler profiler;
    private MutableQueryTimeout cancellable;
    private SearchContext searchContext;

    public ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        boolean wrapWithExitableDirectoryReader,
        Executor executor,
        SearchContext searchContext
    ) throws IOException {
        this(
            reader,
            similarity,
            queryCache,
            queryCachingPolicy,
            new MutableQueryTimeout(),
            wrapWithExitableDirectoryReader,
            executor,
            searchContext
        );
    }

    private ContextIndexSearcher(
        IndexReader reader,
        Similarity similarity,
        QueryCache queryCache,
        QueryCachingPolicy queryCachingPolicy,
        MutableQueryTimeout cancellable,
        boolean wrapWithExitableDirectoryReader,
        Executor executor,
        SearchContext searchContext
    ) throws IOException {
        super(wrapWithExitableDirectoryReader ? new ExitableDirectoryReader((DirectoryReader) reader, cancellable) : reader, executor);
        setSimilarity(similarity);
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
        this.cancellable = cancellable;
        this.searchContext = searchContext;
    }

    public void setProfiler(QueryProfiler profiler) {
        this.profiler = profiler;
    }

    /**
     * Add a {@link Runnable} that will be run on a regular basis while accessing documents in the
     * DirectoryReader but also while collecting them and check for query cancellation or timeout.
     */
    public Runnable addQueryCancellation(Runnable action) {
        return this.cancellable.add(action);
    }

    /**
     * Remove a {@link Runnable} that checks for query cancellation or timeout
     * which is called while accessing documents in the DirectoryReader but also while collecting them.
     */
    public void removeQueryCancellation(Runnable action) {
        this.cancellable.remove(action);
    }

    @Override
    public void close() {
        // clear the list of cancellables when closing the owning search context, since the ExitableDirectoryReader might be cached (for
        // instance in fielddata cache).
        // A cancellable can contain an indirect reference to the search context, which potentially retains a significant amount
        // of memory.
        this.cancellable.clear();
    }

    public boolean hasCancellations() {
        return this.cancellable.isEnabled();
    }

    public void setAggregatedDfs(AggregatedDfs aggregatedDfs) {
        this.aggregatedDfs = aggregatedDfs;
    }

    @Override
    public Query rewrite(Query original) throws IOException {
        if (profiler != null) {
            profiler.startRewriteTime();
        }

        try {
            return super.rewrite(original);
        } finally {
            if (profiler != null) {
                profiler.stopAndAddRewriteTime();
            }
        }
    }

    @Override
    public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
        if (profiler != null) {
            // createWeight() is called for each query in the tree, so we tell the queryProfiler
            // each invocation so that it can build an internal representation of the query
            // tree
            ContextualProfileBreakdown<QueryTimingType> profile = profiler.getQueryBreakdown(query);
            Timer timer = profile.getTimer(QueryTimingType.CREATE_WEIGHT);
            timer.start();
            final Weight weight;
            try {
                weight = query.createWeight(this, scoreMode, boost);
            } finally {
                timer.stop();
                profiler.pollLastElement();
            }
            return new ProfileWeight(query, weight, profile);
        } else if (query instanceof ApproximateScoreQuery) {
            ((ApproximateScoreQuery) query).setContext(searchContext);
            return super.createWeight(query, scoreMode, boost);
        } else {
            return super.createWeight(query, scoreMode, boost);
        }
    }

    public void search(
        List<LeafReaderContext> leaves,
        Weight weight,
        CollectorManager manager,
        QuerySearchResult result,
        DocValueFormat[] formats,
        TotalHits totalHits
    ) throws IOException {
        final List<Collector> collectors = new ArrayList<>(leaves.size());
        for (LeafReaderContext ctx : leaves) {
            final Collector collector = manager.newCollector();
            searchLeaf(ctx, 0, DocIdSetIterator.NO_MORE_DOCS, weight, collector);
            collectors.add(collector);
        }

        TopFieldDocs mergedTopDocs = (TopFieldDocs) manager.reduce(collectors);
        // Lucene sets shards indexes during merging of topDocs from different collectors
        // We need to reset shard index; OpenSearch will set shard index later during reduce stage
        for (ScoreDoc scoreDoc : mergedTopDocs.scoreDocs) {
            scoreDoc.shardIndex = -1;
        }
        if (totalHits != null) { // we have already precalculated totalHits for the whole index
            mergedTopDocs = new TopFieldDocs(totalHits, mergedTopDocs.scoreDocs, mergedTopDocs.fields);
        }
        result.topDocs(new TopDocsAndMaxScore(mergedTopDocs, Float.NaN), formats);
    }

    @Override
    public void search(Query query, Collector collector) throws IOException {
        super.search(query, collector);
        searchContext.bucketCollectorProcessor().processPostCollection(collector);
    }

    public void search(
        Query query,
        CollectorManager<?, TopFieldDocs> manager,
        QuerySearchResult result,
        DocValueFormat[] formats,
        TotalHits totalHits
    ) throws IOException {
        TopFieldDocs mergedTopDocs = search(query, manager);
        // Lucene sets shards indexes during merging of topDocs from different collectors
        // We need to reset shard index; OpenSearch will set shard index later during reduce stage
        for (ScoreDoc scoreDoc : mergedTopDocs.scoreDocs) {
            scoreDoc.shardIndex = -1;
        }
        if (totalHits != null) { // we have already precalculated totalHits for the whole index
            mergedTopDocs = new TopFieldDocs(totalHits, mergedTopDocs.scoreDocs, mergedTopDocs.fields);
        }
        result.topDocs(new TopDocsAndMaxScore(mergedTopDocs, Float.NaN), formats);
    }

    @Override
    protected void search(LeafReaderContextPartition[] partitions, Weight weight, Collector collector) throws IOException {
        searchContext.indexShard().getSearchOperationListener().onPreSliceExecution(searchContext);
        try {
            // Time series based workload by default traverses segments in desc order i.e. latest to the oldest order.
            // This is actually beneficial for search queries to start search on latest segments first for time series workload.
            // That can slow down ASC order queries on timestamp workload. So to avoid that slowdown, we will reverse leaf
            // reader order here.
            if (searchContext.shouldUseTimeSeriesDescSortOptimization()) {
                for (int i = partitions.length - 1; i >= 0; i--) {
                    searchLeaf(partitions[i].ctx, partitions[i].minDocId, partitions[i].maxDocId, weight, collector);
                }
            } else {
                for (LeafReaderContextPartition partition : partitions) {
                    searchLeaf(partition.ctx, partition.minDocId, partition.maxDocId, weight, collector);
                }
            }
            searchContext.bucketCollectorProcessor().processPostCollection(collector);
        } catch (Throwable t) {
            searchContext.indexShard().getSearchOperationListener().onFailedSliceExecution(searchContext);
            throw t;
        }
        searchContext.indexShard().getSearchOperationListener().onSliceExecution(searchContext);
    }

    private void searchParquet(String parquetPath, LeafCollector leafCollector) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            // Create dataset factory for Parquet file
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                parquetPath
            );

            // Get the dataset
            Dataset dataset = factory.finish();

            // Create scan options
            ScanOptions options = new ScanOptions(/*batchSize=*/32768, /*columns=*/Optional.of(getRequiredColumns()) // List of columns you
                                                                                                                     // need
            // /*filter=*/createFilter() // Your search criteria
            );

            // Create scanner
            Scanner scanner = dataset.newScan(options);
            ArrowReader reader = scanner.scanBatches();
            // Process batches
            int rowCount = 0;
            while (reader.loadNextBatch()) {
                try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                    processArrowBatch(root, leafCollector, rowCount);
                    rowCount += root.getRowCount();
                }

            }

        } catch (Exception e) {
            logger.error("Error reading Parquet file with Arrow: " + e.getMessage(), e);
            throw new IOException("Failed to read Parquet file", e);
        }
    }

    private void processArrowBatch(VectorSchemaRoot batch, LeafCollector collector, int rowCount) throws IOException {

        // Get vectors for each field you need
        var backendIpVector = batch.getVector("backend_ip");
        var backendPortVector = batch.getVector("backend_port");
        var timestampVector = batch.getVector("timestamp");
        // ... get other vectors

        // Process each row in the batch
        for (int i = 0; i < batch.getRowCount(); i++) {
            if (matchesSearchCriteria(batch, i)) {
                // Create a scorer for the current row
                // Scorer scorer = new ArrowScorer(batch, i);
                // collector.setScorer(scorer);

                // Generate a document ID for this row
                int docId = rowCount++;
                collector.collect(docId);
            }
        }
    }

    private boolean matchesSearchCriteria(VectorSchemaRoot batch, int rowIndex) {
        try {
            if (searchContext.query() != null) {
                return evaluateQueryOnArrowRow(batch, rowIndex, searchContext.query());
            }
            return true;
        } catch (Exception e) {
            logger.error("Error evaluating search criteria: " + e.getMessage(), e);
            return false;
        }
    }

    private boolean evaluateQueryOnArrowRow(VectorSchemaRoot batch, int rowIndex, Query query) {
        // Implement query evaluation logic
        if (query instanceof TermQuery) {
            TermQuery termQuery = (TermQuery) query;
            String field = termQuery.getTerm().field();
            String value = termQuery.getTerm().text();

            ValueVector vector = batch.getVector(field);
            if (vector instanceof VarCharVector) {
                VarCharVector varCharVector = (VarCharVector) vector;
                return value.equals(new String(varCharVector.get(rowIndex)));
            }
            // Add handling for other vector types
        }
        // Add handling for other query types
        return false;
    }

    private String[] getRequiredColumns() {
        // Return list of columns needed for the query
        String[] a = new String[2];
        return List.of("target_status_code", "target_port").toArray(a);
    }

    private ArrowQueryContext getArrowQueryContext() {
        return searchContext.getQueryShardContext().getArrowQueryContext();
    }

    public static void testParquetExec() throws Exception {
        try (SessionContext context = SessionContexts.create();
             BufferAllocator allocator = new RootAllocator()) {
            ParquetExec exec = new ParquetExec(context, context.getPointer());
            CompletableFuture<RecordBatchStream> result =
                exec.execute(
                    "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet",
                    "target_status_code",
                    404,
                    allocator);
            RecordBatchStream stream = result.join();
            VectorSchemaRoot root = stream.getVectorSchemaRoot();
            while (stream.loadNextBatch().get()) {
                List<FieldVector> vectors = root.getFieldVectors();
                for (FieldVector vector : vectors) {
                    logger.info(
                        "Field - {}, {}, {}, {}",
                        vector.getField().getName(),
                        vector.getField().getType(),
                        vector.getValueCount(),
                        vector);
                }
            }
            stream.close();
        }
    }

    private static void consumeReader(ArrowReader reader) {
        try {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                VarCharVector nameVector = (VarCharVector) root.getVector(0);
                logger.info(
                    "name vector size {}, row count {}, value={}",
                    nameVector.getValueCount(),
                    root.getRowCount(),
                    nameVector);
                BigIntVector ageVector = (BigIntVector) root.getVector(1);
                logger.info(
                    "age vector size {}, row count {}, value={}",
                    ageVector.getValueCount(),
                    root.getRowCount(),
                    ageVector);
            }
            reader.close();
        } catch (IOException e) {
            logger.warn("got IO Exception", e);
        }
    }

    private static CompletableFuture<Void> loadConstant(SessionContext context) {
        return context
            .sql("select 1 + 2")
            .thenComposeAsync(
                dataFrame -> {
                    logger.info("successfully loaded data frame {}", dataFrame);
                    return dataFrame.show();
                });
    }

    /**
     * Lower-level search API.
     * <p>
     * {@link LeafCollector#collect(int)} is called for every matching document in
     * the provided <code>ctx</code>.
     */
    @Override
    protected void searchLeaf(LeafReaderContext ctx, int minDocId, int maxDocId, Weight weight, Collector collector) throws IOException {

        // Check if we have an Arrow context and early terminate
        // TODO : only sum aggs is currently supported
        // TODO : only range and terms query is currently supported
//        try {
//            testParquetExec();
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//        logger.info("arrow.enable_unsafe_memory_access: {}",
//            System.getProperty("arrow.enable_unsafe_memory_access"));
//        logger.info("arrow.enable_null_check_for_get: {}",
//            System.getProperty("arrow.enable_null_check_for_get"));
        ArrowQueryContext arrowCtx = getArrowQueryContext();
        if (arrowCtx != null) {
            if (collector instanceof MultiCollector) {
                for (Collector c : ((MultiCollector) collector).getCollectors()) {
                    if (c instanceof ArrowBatchCollector) {
                        if(arrowCtx.isUsingParquetExec()) {
                            searchWithParquetExec(arrowCtx, c);
                        }
                        else if (arrowCtx.isUsingSubstrait()) {
                            searchWithSubstrait(arrowCtx, c);
                        } else {
                            searchWithArrow(arrowCtx, c);
                        }
                    }
                }
            }
            return;
        }

        // Check if at all we need to call this leaf for collecting results.
        if (canMatch(ctx) == false) {
            return;
        }

        final LeafCollector leafCollector;
        try {
            cancellable.checkCancelled();
            if (weight instanceof ProfileWeight) {
                ((ProfileWeight) weight).associateCollectorToLeaves(ctx, collector);
            }
            weight = wrapWeight(weight);
            // See please https://github.com/apache/lucene/pull/964
            collector.setWeight(weight);
            leafCollector = collector.getLeafCollector(ctx);
        } catch (CollectionTerminatedException e) {
            // there is no doc of interest in this reader context
            // continue with the following leaf
            return;
        } catch (QueryPhase.TimeExceededException e) {
            searchContext.setSearchTimedOut(true);
            return;
        }

        // catch early terminated exception and rethrow?
        Bits liveDocs = ctx.reader().getLiveDocs();
        BitSet liveDocsBitSet = getSparseBitSetOrNull(liveDocs);
        if (liveDocsBitSet == null) {
            BulkScorer bulkScorer = weight.bulkScorer(ctx);
            if (bulkScorer != null) {
                try {
                    bulkScorer.score(leafCollector, liveDocs, minDocId, maxDocId);
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                } catch (QueryPhase.TimeExceededException e) {
                    searchContext.setSearchTimedOut(true);
                    return;
                }
            }
        } else {
            // if the role query result set is sparse then we should use the SparseFixedBitSet for advancing:
            Scorer scorer = weight.scorer(ctx);
            if (scorer != null) {
                try {
                    intersectScorerAndBitSet(
                        scorer,
                        liveDocsBitSet,
                        leafCollector,
                        minDocId,
                        maxDocId,
                        this.cancellable.isEnabled() ? cancellable::checkCancelled : () -> {}
                    );
                } catch (CollectionTerminatedException e) {
                    // collection was terminated prematurely
                    // continue with the following leaf
                } catch (QueryPhase.TimeExceededException e) {
                    searchContext.setSearchTimedOut(true);
                    return;
                }
            }
        }

        // Note: this is called if collection ran successfully, including the above special cases of
        // CollectionTerminatedException and TimeExceededException, but no other exception.
        leafCollector.finish();
    }



    private void searchWithParquetExec(ArrowQueryContext arrowCtx, Collector collector) throws IOException {
        try {
            ParquetExecQueryContext parquetCtx = arrowCtx.getParquetExecContext();
            ParquetExec exec = parquetCtx.getParquetExec();

            // Execute query using ParquetExec
//            CompletableFuture<RecordBatchStream> result = exec.execute(
//                parquetCtx.getParquetPath(),
//                getTargetField(arrowCtx.getBaseQueryBuilder()),
//                Integer.parseInt((String)getTargetValue(arrowCtx.getBaseQueryBuilder())),
//                parquetCtx.getAllocator()
//            );

            // Execute query using ParquetExec
            CompletableFuture<RecordBatchStream> result = exec.execute(
                "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet",
                getTargetField(arrowCtx.getBaseQueryBuilder()),
                (Integer) NumberFieldMapper.NumberType.INTEGER.parse(getTargetValue(arrowCtx.getBaseQueryBuilder()), true),
                parquetCtx.getAllocator());

            // Process results
            RecordBatchStream stream = result.join();
            try {
                VectorSchemaRoot root = stream.getVectorSchemaRoot();
                while (stream.loadNextBatch().get()) {
                    if (collector instanceof ArrowBatchCollector) {
                        if(root.getRowCount() == 0) continue;
                        ((ArrowBatchCollector) collector).collectBatch(root);
//                         for(int i=0; i<root.getRowCount();i++) {
//                         ((ArrowBatchCollector) collector).collect(root, i);
//                         }
                    }
                }
            } finally {
                stream.close();
            }
        } catch (Exception e) {
            logger.error("Error processing data with ParquetExec: " + e.getMessage(), e);
            throw new IOException("Failed to process data with ParquetExec", e);
        }
    }

    private String getTargetField(QueryBuilder query) {
        if (query instanceof TermQueryBuilder) {
            return ((TermQueryBuilder) query).fieldName();
        }
        throw new IllegalArgumentException("Unsupported query type for ParquetExec");
    }

    private Object getTargetValue(QueryBuilder query) {
        if (query instanceof TermQueryBuilder) {
            return ((TermQueryBuilder) query).value();
        }
        throw new IllegalArgumentException("Unsupported query type for ParquetExec");
    }

    private void searchWithSubstrait(ArrowQueryContext arrowCtx, Collector collector) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                arrowCtx.getParquetPath()
            );

            Dataset dataset = factory.finish();

            // Get the Substrait provider and expressions
            SubstraitFilterProvider provider = SubstraitFilterProvider.SingletonFactory.getProvider(arrowCtx.getBaseQueryBuilder());
            if (provider == null) {
                // Fall back to Arrow-based filtering
                searchWithArrow(arrowCtx, collector);
                return;
            }

            // Create scan options with Substrait expressions
            ScanOptions.Builder optionsBuilder = new ScanOptions.Builder(32768);

            // Add filter and projection if available
            ExtendedExpression filter = arrowCtx.getSubstraitFilter();
            ExtendedExpression projection = arrowCtx.getSubstraitProjection();
            List<String> columns = new ArrayList();
            columns.add("target_status_code");
            columns.add("timestamp");
            optionsBuilder.columns(Optional.of(columns.toArray(new String[0])));
            if (filter != null) {
                optionsBuilder.substraitFilter(arrowCtx.convertToByteBuffer(filter));
            }
            if (projection != null) {
                optionsBuilder.substraitProjection(arrowCtx.convertToByteBuffer(projection));
            }

            try (Scanner scanner = dataset.newScan(optionsBuilder.build()); ArrowReader reader = scanner.scanBatches()) {

                while (reader.loadNextBatch()) {
                    try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                        if (collector instanceof ArrowBatchCollector) {
                             for(int i=0; i<root.getRowCount();i++) {
                             ((ArrowBatchCollector) collector).collect(root, i);
                             }
//                            if(root.getRowCount() == 0) continue;
//                            ((ArrowBatchCollector) collector).collectBatch(root);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing data with Substrait: " + e.getMessage(), e);
            throw new IOException("Failed to process data with Substrait", e);
        }
    }

    private void searchWithArrow(ArrowQueryContext arrowCtx, Collector collector) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                arrowCtx.getParquetPath()
            );

            Dataset dataset = factory.finish();
            ArrowFilter baseFilter = arrowCtx.getBaseQueryArrowFilter();

            // Create scan options
            String[] columns = baseFilter.getFilteredFields().toArray(new String[0]);
            ScanOptions options = new ScanOptions(32768, Optional.of(columns));

            try (Scanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {

                int rowCount = 0;
                while (reader.loadNextBatch()) {
                    try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                        processArrowBatchWithFilter(root, collector, rowCount, baseFilter);
                        rowCount += root.getRowCount();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing data with Arrow: " + e.getMessage(), e);
            throw new IOException("Failed to process data with Arrow", e);
        }
    }

    // Modify ContextIndexSearcher to use Substrait
    private void searchArrowSubstrait(ArrowQueryContext arrowCtx, Collector collector, LeafReaderContext ctx) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                arrowCtx.getParquetPath()
            );

            Dataset dataset = factory.finish();

            // Create scan options with Substrait expressions
            ScanOptions options = new ScanOptions.Builder(32768).columns(Optional.of(new String[] { "target_status_code" })) // TODO
                .substraitFilter(/*arrowCtx.getSubstraitFilter()*/null)
                .substraitProjection(/*arrowCtx.getSubstraitProjection()*/null)
                .build();
            try (Scanner scanner = dataset.newScan(options); ArrowReader reader = scanner.scanBatches()) {

                int rowCount = 0;
                while (reader.loadNextBatch()) {
                    try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                        // With Substrait, filtering is done at storage layer
                        // Just collect all rows in the batch
                        ((ArrowBatchCollector) collector).collectBatch(root);
                        rowCount += root.getRowCount();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Arrow/Parquet data: " + e.getMessage(), e);
            throw new IOException("Failed to process Arrow/Parquet data", e);
        }
    }

    // Add new method for Arrow-based search
    private void searchArrow(ArrowQueryContext arrowCtx, Collector collector) throws IOException {
        try (BufferAllocator allocator = new RootAllocator()) {
            DatasetFactory factory = new FileSystemDatasetFactory(
                allocator,
                NativeMemoryPool.getDefault(),
                FileFormat.PARQUET,
                arrowCtx.getParquetPath()
            );

            Dataset dataset = factory.finish();

            // Get base filter from ArrowQueryContext
            ArrowFilter baseFilter = arrowCtx.getBaseQueryArrowFilter();

            String[] arr = new String[baseFilter.getFilteredFields().size()];
            baseFilter.getFilteredFields().toArray(arr);
            // Create scan options with filter
            ScanOptions options = new ScanOptions(/*batchSize=*/32768, /*columns=*/Optional.of(baseFilter.getFilteredFields().toArray(arr))
            );

            try (Scanner scanner = dataset.newScan(options)) {
                ArrowReader reader = scanner.scanBatches();
                int rowCount = 0;

                while (reader.loadNextBatch()) {
                    try (VectorSchemaRoot root = reader.getVectorSchemaRoot()) {
                        // Apply filter and collect results
                        processArrowBatchWithFilter(root, collector, rowCount, baseFilter);
                        rowCount += root.getRowCount();
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error processing Arrow/Parquet data: " + e.getMessage(), e);
            throw new IOException("Failed to process Arrow/Parquet data", e);
        }
    }

    private void processArrowBatchWithFilter(VectorSchemaRoot batch, Collector collector, int rowCount, ArrowFilter filter)
        throws IOException {
        // TODO : this is not needed , if we collect batch as we pass batch inside
        searchContext.getQueryShardContext().getArrowQueryContext().setCurrentBatch(batch);
        try {
            // Create combined bitmap of all matching rows
            RoaringBitmap matches = null;
            // Apply filters for each field
            for (String field : filter.getFilteredFields()) {
                matches = filter.evaluate(field, batch);
            }

            if (collector instanceof ArrowBatchCollector) {
                ((ArrowBatchCollector) collector).collectBatch(batch, matches, rowCount);
            } else {
                // Use explicit IntConsumer
                // matches.forEach((IntConsumer) i -> {
                // try {
                // collector.collect(i);
                // } catch (IOException e) {
                // throw new RuntimeException(e);
                // }
                // });
            }
        } finally {
            // Clear the batch reference
            searchContext.getQueryShardContext().getArrowQueryContext().setCurrentBatch(null);
        }
    }

    private Weight wrapWeight(Weight weight) {
        if (cancellable.isEnabled()) {
            return new Weight(weight.getQuery()) {

                @Override
                public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                    throw new UnsupportedOperationException();
                }

                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                    return new ScorerSupplier() {
                        private Scorer scorer;
                        private BulkScorer bulkScorer;

                        @Override
                        public Scorer get(long leadCost) throws IOException {
                            scorer = weight.scorer(context);
                            return scorer;
                        }

                        @Override
                        public BulkScorer bulkScorer() throws IOException {
                            final BulkScorer in = weight.bulkScorer(context);

                            if (in != null) {
                                bulkScorer = new CancellableBulkScorer(in, cancellable::checkCancelled);
                            } else {
                                bulkScorer = null;
                            }

                            return bulkScorer;
                        }

                        @Override
                        public long cost() {
                            if (scorer != null) {
                                return scorer.iterator().cost();
                            } else if (bulkScorer != null) {
                                return bulkScorer.cost();
                            } else {
                                // We have no prior knowledge of how many docs might match for any given query term,
                                // so we assume that all docs could be a match.
                                return Integer.MAX_VALUE;
                            }
                        }
                    };
                }

                @Override
                public int count(LeafReaderContext context) throws IOException {
                    return weight.count(context);
                }
            };
        } else {
            return weight;
        }
    }

    private static BitSet getSparseBitSetOrNull(Bits liveDocs) {
        if (liveDocs instanceof SparseFixedBitSet) {
            return (BitSet) liveDocs;
        } else if (liveDocs instanceof CombinedBitSet
            // if the underlying role bitset is sparse
            && ((CombinedBitSet) liveDocs).getFirst() instanceof SparseFixedBitSet) {
                return (BitSet) liveDocs;
            } else {
                return null;
            }

    }

    static void intersectScorerAndBitSet(Scorer scorer, BitSet acceptDocs, LeafCollector collector, Runnable checkCancelled)
        throws IOException {
        intersectScorerAndBitSet(scorer, acceptDocs, collector, 0, DocIdSetIterator.NO_MORE_DOCS, checkCancelled);
    }

    private static void intersectScorerAndBitSet(
        Scorer scorer,
        BitSet acceptDocs,
        LeafCollector collector,
        int minDocId,
        int maxDocId,
        Runnable checkCancelled
    ) throws IOException {
        collector.setScorer(scorer);
        // ConjunctionDISI uses the DocIdSetIterator#cost() to order the iterators, so if roleBits has the lowest cardinality it should
        // be used first:
        DocIdSetIterator iterator = ConjunctionUtils.intersectIterators(
            Arrays.asList(new BitSetIterator(acceptDocs, acceptDocs.approximateCardinality()), scorer.iterator())
        );
        int seen = 0;
        checkCancelled.run();
        iterator.advance(minDocId);
        for (int docId = iterator.docID(); docId < maxDocId; docId = iterator.nextDoc()) {
            if (++seen % CHECK_CANCELLED_SCORER_INTERVAL == 0) {
                checkCancelled.run();
            }
            collector.collect(docId);
        }
        checkCancelled.run();
    }

    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        TermStatistics termStatistics = aggregatedDfs.termStatistics().get(term);
        if (termStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.termStatistics(term, docFreq, totalTermFreq);
        }
        return termStatistics;
    }

    @Override
    public CollectionStatistics collectionStatistics(String field) throws IOException {
        if (aggregatedDfs == null) {
            // we are either executing the dfs phase or the search_type doesn't include the dfs phase.
            return super.collectionStatistics(field);
        }
        CollectionStatistics collectionStatistics = aggregatedDfs.fieldStatistics().get(field);
        if (collectionStatistics == null) {
            // we don't have stats for this - this might be a must_not clauses etc. that doesn't allow extract terms on the query
            return super.collectionStatistics(field);
        }
        return collectionStatistics;
    }

    /**
     * Compute the leaf slices that will be used by concurrent segment search to spread work across threads
     * @param leaves all the segments
     * @return leafSlice group to be executed by different threads
     */
    @Override
    protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
        return slicesInternal(leaves, searchContext.getTargetMaxSliceCount());
    }

    public DirectoryReader getDirectoryReader() {
        final IndexReader reader = getIndexReader();
        assert reader instanceof DirectoryReader : "expected an instance of DirectoryReader, got " + reader.getClass();
        return (DirectoryReader) reader;
    }

    private static class MutableQueryTimeout implements ExitableDirectoryReader.QueryCancellation {

        private final Set<Runnable> runnables = new HashSet<>();

        private Runnable add(Runnable action) {
            Objects.requireNonNull(action, "cancellation runnable should not be null");
            if (runnables.add(action) == false) {
                throw new IllegalArgumentException("Cancellation runnable already added");
            }
            return action;
        }

        private void remove(Runnable action) {
            runnables.remove(action);
        }

        @Override
        public void checkCancelled() {
            for (Runnable timeout : runnables) {
                timeout.run();
            }
        }

        @Override
        public boolean isEnabled() {
            return runnables.isEmpty() == false;
        }

        public void clear() {
            runnables.clear();
        }
    }

    private boolean canMatch(LeafReaderContext ctx) throws IOException {
        // skip segments for search after if min/max of them doesn't qualify competitive
        return canMatchSearchAfter(ctx);
    }

    private boolean canMatchSearchAfter(LeafReaderContext ctx) throws IOException {
        if (searchContext.searchAfter() != null && searchContext.request() != null && searchContext.request().source() != null) {
            // Only applied on primary sort field and primary search_after.
            FieldSortBuilder primarySortField = FieldSortBuilder.getPrimaryFieldSortOrNull(searchContext.request().source());
            if (primarySortField != null) {
                MinAndMax<?> minMax = FieldSortBuilder.getMinMaxOrNullForSegment(
                    this.searchContext.getQueryShardContext(),
                    ctx,
                    primarySortField,
                    searchContext.sort()
                );
                return SearchService.canMatchSearchAfter(
                    searchContext.searchAfter(),
                    minMax,
                    primarySortField,
                    searchContext.trackTotalHitsUpTo()
                );
            }
        }
        return true;
    }

    // package-private for testing
    LeafSlice[] slicesInternal(List<LeafReaderContext> leaves, int targetMaxSlice) {
        LeafSlice[] leafSlices;
        if (targetMaxSlice == 0) {
            // use the default lucene slice calculation
            leafSlices = super.slices(leaves);
            logger.debug("Slice count using lucene default [{}]", leafSlices.length);
        } else {
            // use the custom slice calculation based on targetMaxSlice
            leafSlices = MaxTargetSliceSupplier.getSlices(leaves, targetMaxSlice);
            logger.debug("Slice count using max target slice supplier [{}]", leafSlices.length);
        }
        return leafSlices;
    }
}
