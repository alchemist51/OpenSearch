/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.Map;

import org.roaringbitmap.RoaringBitmap;

public class RangeArrowFilterProvider implements ArrowFilterProvider {
    @Override
    public ArrowFilter getFilter(SearchContext context, QueryBuilder rawFilter) {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) rawFilter;
        String field = rangeQuery.fieldName();

        return new ArrowFilter(Map.of(field, root -> {
            ValueVector vector = root.getVector(field);

            if (vector == null) return null;

            // Handle different vector types
            if (vector instanceof IntVector) {
                return filterIntVector(
                    (IntVector) vector,
                    parseValue(rangeQuery.from(), Integer.MIN_VALUE),
                    parseValue(rangeQuery.to(), Integer.MAX_VALUE),
                    rangeQuery.includeLower(),
                    rangeQuery.includeUpper()
                );
            } else if (vector instanceof BigIntVector) {
                return filterLongVector(
                    (BigIntVector) vector,
                    parseValue(rangeQuery.from(), Long.MIN_VALUE),
                    parseValue(rangeQuery.to(), Long.MAX_VALUE),
                    rangeQuery.includeLower(),
                    rangeQuery.includeUpper()
                );
            } else if (vector instanceof Float4Vector) {
                return filterFloatVector(
                    (Float4Vector) vector,
                    parseValue(rangeQuery.from(), Float.NEGATIVE_INFINITY),
                    parseValue(rangeQuery.to(), Float.POSITIVE_INFINITY),
                    rangeQuery.includeLower(),
                    rangeQuery.includeUpper()
                );
            } else if (vector instanceof Float8Vector) {
                return filterDoubleVector(
                    (Float8Vector) vector,
                    parseValue(rangeQuery.from(), Double.NEGATIVE_INFINITY),
                    parseValue(rangeQuery.to(), Double.POSITIVE_INFINITY),
                    rangeQuery.includeLower(),
                    rangeQuery.includeUpper()
                );
            }

            return null;
        }));
    }

    private RoaringBitmap filterIntVector(IntVector vector, int from, int to, boolean includeLower, boolean includeUpper) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) continue;

            int value = vector.get(i);
            if (inRange(value, from, to, includeLower, includeUpper)) {
                bitmap.add(i);
            }
        }
        return bitmap;
    }

    private RoaringBitmap filterLongVector(BigIntVector vector, long from, long to, boolean includeLower, boolean includeUpper) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) continue;

            long value = vector.get(i);
            if (inRange(value, from, to, includeLower, includeUpper)) {
                bitmap.add(i);
            }
        }
        return bitmap;
    }

    private RoaringBitmap filterFloatVector(Float4Vector vector, float from, float to, boolean includeLower, boolean includeUpper) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) continue;

            float value = vector.get(i);
            if (inRange(value, from, to, includeLower, includeUpper)) {
                bitmap.add(i);
            }
        }
        return bitmap;
    }

    private RoaringBitmap filterDoubleVector(Float8Vector vector, double from, double to, boolean includeLower, boolean includeUpper) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int i = 0; i < vector.getValueCount(); i++) {
            if (vector.isNull(i)) continue;

            double value = vector.get(i);
            if (inRange(value, from, to, includeLower, includeUpper)) {
                bitmap.add(i);
            }
        }
        return bitmap;
    }

    private <T extends Comparable<T>> boolean inRange(T value, T from, T to, boolean includeLower, boolean includeUpper) {
        int compareToFrom = value.compareTo(from);
        int compareToTo = value.compareTo(to);

        return (includeLower ? compareToFrom >= 0 : compareToFrom > 0) && (includeUpper ? compareToTo <= 0 : compareToTo < 0);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseValue(Object value, T defaultValue) {
        if (value == null) return defaultValue;

        if (defaultValue instanceof Integer) {
            return (T) (Integer) NumberFieldMapper.NumberType.INTEGER.parse(value, true).intValue();
            // return (T) (Integer) ((Number) value).intValue();
            // TODO For others
        } else if (defaultValue instanceof Long) {
            return (T) (Long) ((Number) value).longValue();
        } else if (defaultValue instanceof Float) {
            return (T) (Float) ((Number) value).floatValue();
        } else if (defaultValue instanceof Double) {
            return (T) (Double) ((Number) value).doubleValue();
        }

        return (T) value;
    }
}
