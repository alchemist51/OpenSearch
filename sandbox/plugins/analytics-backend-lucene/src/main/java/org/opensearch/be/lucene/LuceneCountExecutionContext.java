/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.opensearch.analytics.spi.BackendExecutionContext;

import java.util.List;

/**
 * Lucene-driver execution context for the count fast path. Built by
 * {@link LuceneScanInstructionHandler} from a {@code ShardScanInstructionNode} +
 * the compiled {@link Query} (deserialized from the planner-produced
 * {@code BoolQueryBuilder} bytes). Consumed by {@code LuceneCountSearchExecEngine}.
 *
 * <p>Holds no native resources; {@link #close()} is a no-op. The {@link IndexSearcher}'s
 * underlying reader is owned by the caller-acquired {@code ReaderContext}, which closes
 * it after the engine stream drains.
 *
 * @opensearch.internal
 */
final class LuceneCountExecutionContext implements BackendExecutionContext {

    private final IndexSearcher searcher;
    /** May be {@code null} when the fragment has no filter (fast path runs MatchAllDocs). */
    private final Query filterQuery;
    /** Output column names from the planner — typically one per partial-count aggregate call. */
    private final List<String> partialCountColumnNames;

    LuceneCountExecutionContext(IndexSearcher searcher, Query filterQuery, List<String> partialCountColumnNames) {
        this.searcher = searcher;
        this.filterQuery = filterQuery;
        this.partialCountColumnNames = List.copyOf(partialCountColumnNames);
    }

    IndexSearcher searcher() {
        return searcher;
    }

    Query filterQuery() {
        return filterQuery;
    }

    List<String> partialCountColumnNames() {
        return partialCountColumnNames;
    }
}
