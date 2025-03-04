/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

import org.opensearch.search.internal.ParquetBitSet;
import org.roaringbitmap.RoaringBitmap;

/**
 * Interface for batch-oriented collection of Arrow data
 */
public interface ArrowBatchCollector {
    /**
     * Process an entire batch of matched records at once
     * @param root The VectorSchemaRoot containing the batch data
     * @param matchedRows Bitmap of rows that matched the query
     * @param baseDocId The starting document ID for this batch
     */
    void collectBatch(VectorSchemaRoot root, RoaringBitmap matchedRows, long baseDocId) throws IOException;

    void collectBatch(VectorSchemaRoot root) throws IOException;

    void collect(VectorSchemaRoot root, int doc) throws IOException;

    void collect(int value) throws IOException;

    void collect(IntVector intVector, int idx) throws IOException;
    int collectBatch(VectorSchemaRoot root, ParquetBitSet bitSet) throws IOException;
}
