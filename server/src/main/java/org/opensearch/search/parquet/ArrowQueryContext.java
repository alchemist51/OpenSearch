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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.parquet.substrait.SubstraitFilterProvider;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collections;

import io.substrait.proto.ExtendedExpression;

@ExperimentalApi
public class ArrowQueryContext {
    private final QueryBuilder baseQueryBuilder;
    private ArrowFilter baseArrowFilter;
    private final String parquetPath;
    private VectorSchemaRoot currentBatch; // Add this
    private ExtendedExpression substraitFilter;
    private ExtendedExpression substraitProjection;
    private boolean useSubstrait;
    private boolean useParquetExec;
    private boolean useFilterExec;
    private final ParquetExecQueryContext parquetExecContext;

    public ArrowQueryContext(SearchContext context, QueryBuilder baseQueryBuilder, String parquetPath) {
        this.baseQueryBuilder = baseQueryBuilder;
        this.parquetPath = parquetPath;
        this.parquetExecContext = new ParquetExecQueryContext(context, parquetPath);

        initializeFilters(context);
    }

    private void initializeFilters(SearchContext context) {
        // Try Substrait first
        if(baseQueryBuilder instanceof MatchQueryBuilder) {
            this.useParquetExec = true;
            return;
        }

        if (canUseParquetExec(baseQueryBuilder)) {
            this.useParquetExec = true;
            return;
        }
        SubstraitFilterProvider substraitProvider = SubstraitFilterProvider.SingletonFactory.getProvider(baseQueryBuilder);
        if (substraitProvider != null) {
            this.substraitFilter = substraitProvider.getFilter(context, baseQueryBuilder);
            this.substraitProjection = substraitProvider.getProjection(context, baseQueryBuilder);
            this.useSubstrait = true;
        } else {
            // Fall back to Arrow
            consolidateAllFilters(context);
        }
    }

    private boolean canUseParquetExec(QueryBuilder query) {
        // Add logic to determine if query can use ParquetExec
        return query instanceof TermQueryBuilder || query instanceof BoolQueryBuilder || query instanceof MatchQueryBuilder;
    }

    public boolean isUsingSubstrait() {
        return useSubstrait;
    }

    public boolean isUsingParquetExec() {
        return useParquetExec;
        //return false;
    }

    public boolean isUsingFilterExec() {
        return useFilterExec;
    }

    public VectorSchemaRoot getCurrentBatch() {
        return currentBatch;
    }

    public void setCurrentBatch(VectorSchemaRoot batch) {
        this.currentBatch = batch;
    }

    public boolean consolidateAllFilters(SearchContext context) {
        if(baseQueryBuilder instanceof MatchQueryBuilder) {
            return true;
        }

        if (baseQueryBuilder != null) {
            baseArrowFilter = getArrowFilter(context, baseQueryBuilder);
            return baseArrowFilter != null;
        }
        return true;
    }

    public ExtendedExpression getSubstraitFilter() {
        return substraitFilter;
    }

    public ExtendedExpression getSubstraitProjection() {
        return substraitProjection;
    }

    /**
    private void createSubstraitExpressions(SearchContext context) {
        try {
            SqlExpressionToSubstrait expressionToSubstrait = new SqlExpressionToSubstrait();

            // Create schema definition
            String schema = createSchemaDefinition();

            // Convert query to Substrait filter
            if (baseQueryBuilder instanceof TermQueryBuilder) {
                TermQueryBuilder termQuery = (TermQueryBuilder) baseQueryBuilder;
                String filterExpression = String.format("%s = %s",
                    termQuery.fieldName(),
                    termQuery.value().toString());

                ExtendedExpression filter = expressionToSubstrait.convert(
                    new String[]{filterExpression},
                    List.of(schema)
                );
                substraitFilter = convertToByteBuffer(filter);

                // Create projection for required fields
                ExtendedExpression projection = expressionToSubstrait.convert(
                    new String[]{termQuery.fieldName()},
                    List.of(schema)
                );
                substraitProjection = convertToByteBuffer(projection);
            }
            // Add other query types as needed

        } catch (Exception e) {
            throw new RuntimeException("Failed to create Substrait expressions", e);
        }
    }

    private String createSchemaDefinition() {
        return "CREATE TABLE LOGS (" +
            "backend_ip VARCHAR, " +
            "backend_port INTEGER, " +
            "backend_processing_time FLOAT, " +
            "backend_status_code INTEGER, " +
            "client_ip VARCHAR, " +
            "client_port INTEGER, " +
            "connection_time FLOAT, " +
            "destination_ip VARCHAR, " +
            "destination_port INTEGER, " +
            "elb_status_code INTEGER, " +
            "http_port INTEGER, " +
            "http_version VARCHAR, " +
            "matched_rule_priority INTEGER, " +
            "received_bytes INTEGER, " +
            "request_creation_time BIGINT, " +
            "request_processing_time FLOAT, " +
            "response_processing_time FLOAT, " +
            "sent_bytes INTEGER, " +
            "target_ip VARCHAR, " +
            "target_port INTEGER, " +
            "target_processing_time FLOAT, " +
            "target_status_code INTEGER, " +
            "timestamp_col BIGINT)";
    }

    private ByteBuffer convertToByteBuffer(ExtendedExpression expression) {
        byte[] expressionBytes = Base64.getDecoder().decode(
            Base64.getEncoder().encodeToString(expression.toByteArray())
        );
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(expressionBytes.length);
        byteBuffer.put(expressionBytes);
        byteBuffer.flip();
        return byteBuffer;
    }

    public ByteBuffer getSubstraitFilter() {
        return substraitFilter;
    }

    public ByteBuffer getSubstraitProjection() {
        return substraitProjection;
    }
     **/

    public ArrowFilter getBaseQueryArrowFilter() {
        if (baseArrowFilter == null) {
            return new ArrowFilter(Collections.emptyMap());
        }
        return baseArrowFilter;
    }

    public QueryBuilder getBaseQueryBuilder() {
        return baseQueryBuilder;
    }

    public ByteBuffer convertToByteBuffer(ExtendedExpression expression) {
        byte[] expressionBytes = Base64.getDecoder().decode(Base64.getEncoder().encodeToString(expression.toByteArray()));
        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(expressionBytes.length);
        byteBuffer.put(expressionBytes);
        byteBuffer.flip();
        return byteBuffer;
    }

    private ArrowFilter getArrowFilter(SearchContext context, QueryBuilder queryBuilder) {
        ArrowFilterProvider filterProvider = ArrowFilterProvider.SingletonFactory.getProvider(queryBuilder);
        if (filterProvider == null) {
            return null;
        }
        return filterProvider.getFilter(context, queryBuilder);
    }

    public String getParquetPath() {
        return parquetPath;
    }

    public ParquetExecQueryContext getParquetExecContext() {
        return parquetExecContext;
    }
}
