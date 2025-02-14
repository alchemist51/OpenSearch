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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

// Example implementation for Term queries
class TermArrowFilterProvider implements ArrowFilterProvider {
    @Override
    public ArrowFilter getFilter(SearchContext context, QueryBuilder rawFilter) {
        TermQueryBuilder termQuery = (TermQueryBuilder) rawFilter;
        return new ArrowFilter(Map.of(termQuery.fieldName(), root -> {
            ValueVector vector = root.getVector(termQuery.fieldName());
            if (vector instanceof IntVector) {
                return filterIntVector((IntVector) vector, NumberFieldMapper.NumberType.INTEGER.parse(termQuery.value(), true).intValue());
            }
            return null;
        }));
    }

    private RoaringBitmap filterIntVector(IntVector vector, int value) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < vector.getValueCount(); i++) {
            try {
                if (vector.get(i) == value) {
                    bitmap.add(i);
                }
            } catch (IllegalStateException e) {
                System.out.println("This field is null, so continuing");
            }
            // if (!vector.isNull(i) && vector.get(i) == value) {
            // bitmap.add(i);
            // }
        }
        return bitmap;
    }
}
