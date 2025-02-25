/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet.substrait;

import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.util.List;

import io.substrait.isthmus.SqlExpressionToSubstrait;
import io.substrait.proto.ExtendedExpression;

public class RangeSubstraitFilterProvider implements SubstraitFilterProvider {
    @Override
    public ExtendedExpression getFilter(SearchContext context, QueryBuilder rawFilter) {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) rawFilter;
        StringBuilder filterExpression = new StringBuilder();

        // TODO : hardcoded date
        if(rangeQuery.fieldName().equals("timestamp")) {
            filterExpression.append(rangeQuery.fieldName() + "_col");
            long from = ((DateFieldMapper.DateFieldType) context.mapperService().fieldType(rangeQuery.fieldName())).parse(rangeQuery.from().toString());
            long to = ((DateFieldMapper.DateFieldType) context.mapperService().fieldType(rangeQuery.fieldName())).parse(rangeQuery.to().toString());
            filterExpression.append(rangeQuery.includeLower() ? " >= " : " > ").append(from/1000);
            filterExpression.append(" AND ").append(rangeQuery.fieldName() + "_col");
            filterExpression.append(rangeQuery.includeUpper() ? " <= " : " < ").append(to/1000);
        } else {
            filterExpression.append(rangeQuery.fieldName());
            if (rangeQuery.from() != null) {
                filterExpression.append(rangeQuery.includeLower() ? " >= " : " > ").append(rangeQuery.from());
            }

            if (rangeQuery.to() != null) {
                if (rangeQuery.from() != null) {
                    filterExpression.append(" AND ").append(rangeQuery.fieldName());
                }
                filterExpression.append(rangeQuery.includeUpper() ? " <= " : " < ").append(rangeQuery.to());
            }
        }

        return convertToSubstrait(filterExpression.toString(), context);
    }

    @Override
    public ExtendedExpression getProjection(SearchContext context, QueryBuilder rawFilter) {
        RangeQueryBuilder rangeQuery = (RangeQueryBuilder) rawFilter;
        return convertToSubstraitCols(rangeQuery.fieldName(), context);
    }

    private String formatValue(Object value) {
        if (value instanceof String) {
            return "'" + value + "'";
        }
        return value.toString();
    }

    private ExtendedExpression convertToSubstrait(String expression, SearchContext context) {
        try {
            SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait();
            return converter.convert(new String[] { expression }, List.of(SchemaDefinition.createSchemaDefinition()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert to Substrait expression", e);
        }
    }

    private ExtendedExpression convertToSubstraitCols(String expression, SearchContext context) {
        try {
            SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait();
            // TODO : HARDCODED
            return converter.convert(new String[] { "target_status_code", "timestamp_col" }, List.of(SchemaDefinition.createSchemaDefinition()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert to Substrait expression", e);
        }
    }
}
