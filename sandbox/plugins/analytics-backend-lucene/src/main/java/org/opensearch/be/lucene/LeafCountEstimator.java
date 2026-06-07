/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;

/**
 * Returns a cheap, approximate document count for one Lucene query shape on one
 * segment leaf. Built once at handle construction (per delegated leaf), evaluated
 * per call to {@link LuceneFilterDelegationHandle#estimateSelectivityPpm(int, long)}.
 *
 * <p>Implementations dispatch on the underlying {@code Query} subtype:
 * <ul>
 *   <li>plain {@link org.apache.lucene.search.TermQuery} → {@link org.apache.lucene.search.Weight#count(LeafReaderContext)}
 *       (returns {@code docFreq} from the term dictionary)</li>
 *   <li>{@code BooleanQuery} of shape {@code mustNot term} → {@code numDocs - docFreq}
 *       via a single term-dictionary lookup (Lucene's own {@code BooleanWeight.count}
 *       returns {@code -1} for any query containing a {@code MUST_NOT} clause)</li>
 *   <li>anything else → returns {@code -1} so the driver consults to be safe</li>
 * </ul>
 */
interface LeafCountEstimator {
    /**
     * Returns a non-negative count of matching documents in {@code leaf}, or
     * {@code -1} if the count can't be cheaply derived. Implementations must be
     * idempotent and free of per-leaf caching state — they're called per RG.
     */
    long estimate(LeafReaderContext leaf) throws IOException;
}
