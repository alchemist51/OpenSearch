/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.roaringbitmap.RoaringBitmap;

@ExperimentalApi
public class ArrowFilter {
    private final Map<String, Function<VectorSchemaRoot, RoaringBitmap>> fieldFilters;

    public ArrowFilter(Map<String, Function<VectorSchemaRoot, RoaringBitmap>> fieldFilters) {
        this.fieldFilters = fieldFilters;
    }

    public RoaringBitmap evaluate(String field, VectorSchemaRoot root) {
        Function<VectorSchemaRoot, RoaringBitmap> filter = fieldFilters.get(field);
        return filter != null ? filter.apply(root) : null;
    }

    public boolean hasFilter(String field) {
        return fieldFilters.containsKey(field);
    }

    public Set<String> getFilteredFields() {
        return fieldFilters.keySet();
    }
}
