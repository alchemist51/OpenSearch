/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet.substrait;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.Map;

import io.substrait.proto.ExtendedExpression;

public interface SubstraitFilterProvider {
    ExtendedExpression getFilter(SearchContext context, QueryBuilder rawFilter);

    ExtendedExpression getProjection(SearchContext context, QueryBuilder rawFilter);

    class SingletonFactory {
        private static final Map<String, SubstraitFilterProvider> QUERY_BUILDERS_TO_FILTER_PROVIDER = Map.of(
            TermQueryBuilder.NAME,
            new TermSubstraitFilterProvider(),
            RangeQueryBuilder.NAME,
            new RangeSubstraitFilterProvider()
        );

        public static SubstraitFilterProvider getProvider(QueryBuilder query) {
            return query != null ? QUERY_BUILDERS_TO_FILTER_PROVIDER.get(query.getName()) : null;
        }
    }
}
