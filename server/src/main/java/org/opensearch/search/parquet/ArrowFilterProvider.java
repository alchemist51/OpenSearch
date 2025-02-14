/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.Map;

public interface ArrowFilterProvider {
    ArrowFilter getFilter(SearchContext context, QueryBuilder rawFilter);

    class SingletonFactory {
        private static final Map<String, ArrowFilterProvider> QUERY_BUILDERS_TO_FILTER_PROVIDER = Map.of(
            TermQueryBuilder.NAME,
            new TermArrowFilterProvider(),
            RangeQueryBuilder.NAME,
            new RangeArrowFilterProvider()
        );

        public static ArrowFilterProvider getProvider(QueryBuilder query) {
            return query != null ? QUERY_BUILDERS_TO_FILTER_PROVIDER.get(query.getName()) : null;
        }
    }
}
