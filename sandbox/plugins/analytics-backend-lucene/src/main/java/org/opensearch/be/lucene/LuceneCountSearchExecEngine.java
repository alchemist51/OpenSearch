/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineResultStream;
import org.opensearch.analytics.backend.SearchExecEngine;
import org.opensearch.analytics.backend.ShardScanExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Lucene-driver search execution engine for the count fast path. Runs
 * {@link org.apache.lucene.search.IndexSearcher#count(org.apache.lucene.search.Query)},
 * exports the answer through the Arrow C-Data interface, and returns a
 * {@link LuceneCountResultStream} that materialises the batch via
 * {@link Data#importIntoVectorSchemaRoot} — exactly the way DataFusion's
 * {@code DatafusionResultStream.BatchIterator.loadNextBatch} produces its result VSRs.
 *
 * <p>Going through the C-Data round-trip (rather than handing Flight a pure-Java
 * {@code setSafe}-built VSR) is what makes this batch survive
 * {@code VectorTransfer.transferRoot} cleanly — see the class javadoc on
 * {@link LuceneCountResultStream} for the detailed comparison.
 *
 * <p>No deletes gate. {@code IndexSearcher.count} is self-healing: per-leaf
 * {@code Weight.count(leaf)} returns -1 on dirty leaves and falls back to full iteration —
 * correct under deletes, just slower. Driving from Lucene means the slow case is "as slow
 * as Lucene's iterate," which is still substantially faster than DataFusion decoding rows.
 *
 * @opensearch.internal
 */
final class LuceneCountSearchExecEngine implements SearchExecEngine<ShardScanExecutionContext, EngineResultStream> {

    private static final Logger LOGGER = LogManager.getLogger(LuceneCountSearchExecEngine.class);

    private final LuceneCountExecutionContext luceneCtx;

    LuceneCountSearchExecEngine(LuceneCountExecutionContext luceneCtx) {
        this.luceneCtx = luceneCtx;
    }

    @Override
    public void prepare(ShardScanExecutionContext context) {
        // No preparation needed — the LuceneCountExecutionContext was fully built by the
        // instruction handler. {@code prepare} is part of the SearchExecEngine contract for
        // backends that need to assemble plans from the context (e.g. DataFusion); Lucene
        // has nothing to assemble at this point.
    }

    @Override
    public EngineResultStream execute(ShardScanExecutionContext context) throws IOException {
        long count = luceneCtx.searcher().count(luceneCtx.filterQuery());
        LOGGER.debug(
            "[lucene-count] shardId={} query={} count={} columns={}",
            context.getShardId(),
            luceneCtx.filterQuery(),
            count,
            luceneCtx.partialCountColumnNames()
        );
        BufferAllocator allocator = context.getAllocator();
        Schema schema = buildSchema(luceneCtx.partialCountColumnNames());
        ArrowArray array = ArrowArray.allocateNew(allocator);
        ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator);
        boolean transferred = false;
        try {
            populateCountBatchToCData(allocator, schema, luceneCtx.partialCountColumnNames(), count, array, arrowSchema);
            LuceneCountResultStream stream = new LuceneCountResultStream(array, arrowSchema, allocator);
            transferred = true;
            return stream;
        } finally {
            if (transferred == false) {
                try {
                    array.close();
                } finally {
                    arrowSchema.close();
                }
            }
        }
    }

    private static Schema buildSchema(List<String> columnNames) {
        FieldType int64Nullable = new FieldType(true, new ArrowType.Int(64, true), null);
        List<Field> fields = new ArrayList<>(columnNames.size());
        for (String name : columnNames) {
            fields.add(new Field(name, int64Nullable, null));
        }
        return new Schema(fields);
    }

    /**
     * Builds a one-row scratch VSR carrying {@code count} for every column, exports it to the
     * supplied {@code array}/{@code arrowSchema} pair via the Arrow C-Data interface, then
     * closes the scratch VSR. Mirrors the export side of the DataFusion result-stream contract:
     * the populated {@link ArrowArray} is what {@link LuceneCountResultStream} re-imports into
     * its result VSR — same call shape DataFusion uses for native record batches.
     */
    private static void populateCountBatchToCData(
        BufferAllocator allocator,
        Schema schema,
        List<String> columnNames,
        long count,
        ArrowArray array,
        ArrowSchema arrowSchema
    ) {
        VectorSchemaRoot scratch = VectorSchemaRoot.create(schema, allocator);
        try {
            scratch.allocateNew();
            for (int i = 0; i < columnNames.size(); i++) {
                BigIntVector v = (BigIntVector) scratch.getVector(i);
                v.setSafe(0, count);
            }
            scratch.setRowCount(1);
            try (CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                Data.exportVectorSchemaRoot(allocator, scratch, dictProvider, array, arrowSchema);
            }
        } finally {
            scratch.close();
        }
    }
}
