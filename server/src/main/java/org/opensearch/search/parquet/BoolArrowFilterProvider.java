/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.List;
import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

public class BoolArrowFilterProvider implements ArrowFilterProvider {
    @Override
    public ArrowFilter getFilter(SearchContext context, QueryBuilder rawFilter) {
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) rawFilter;
        List<QueryBuilder> mustClauses = boolQuery.must();
        List<QueryBuilder> shouldClauses = boolQuery.should();
        List<QueryBuilder> mustNotClauses = boolQuery.mustNot();

        // NOTE: Second clause should be a known term or range query -->
        if (mustClauses.size() == 2) {
            return ArrowFilterProvider.SingletonFactory.getProvider(mustClauses.get(1)).getFilter(context, mustClauses.get(1));
        } else if (shouldClauses.size() == 2) {
            return ArrowFilterProvider.SingletonFactory.getProvider(shouldClauses.get(1)).getFilter(context, shouldClauses.get(1));
        } else if (mustNotClauses.size() == 2) {
            return ArrowFilterProvider.SingletonFactory.getProvider(mustNotClauses.get(1)).getFilter(context, mustNotClauses.get(1));
        }

        return null;
    }
}
