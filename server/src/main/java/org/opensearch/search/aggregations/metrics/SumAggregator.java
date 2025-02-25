/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.arrow.flatbuf.Int;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.parquet.ArrowBatchCollector;
import org.opensearch.search.parquet.ArrowQueryContext;
import org.opensearch.search.startree.StarTreeQueryHelper;

import java.io.IOException;
import java.util.Map;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into a single sum value
 *
 * @opensearch.internal
 */
public class SumAggregator extends NumericMetricsAggregator.SingleValue implements StarTreePreComputeCollector, ArrowBatchCollector {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private DoubleArray compensations;

    SumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
            compensations = context.bigArrays().newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return false;
        }
        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context.getQueryShardContext());
        if (supportedStarTree != null) {
            if (parent != null && subAggregators.length == 0) {
                // If this a child aggregator, then the parent will trigger star-tree pre-computation.
                // Returning NO_OP_COLLECTOR explicitly because the getLeafCollector() are invoked starting from innermost aggregators
                return true;
            }
            precomputeLeafUsingStarTree(ctx, supportedStarTree);
            return true;
        }
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {

        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        // Check if we're using Arrow
        ArrowQueryContext arrowCtx = context.getQueryShardContext().getArrowQueryContext();
        if (arrowCtx != null) {
            return getArrowLeafCollector(sub);
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    @Override
    public void collectBatch(VectorSchemaRoot root, RoaringBitmap matchedRows, long baseDocId) throws IOException {
        ValueVector vector = root.getVector("target_status_code");
        if (vector instanceof IntVector) {
            IntVector intVector = (IntVector) vector;

            // Get values for bucket 0 (since this is a single-bucket aggregation)
            sums = context.bigArrays().grow(sums, 1);
            compensations = context.bigArrays().grow(compensations, 1);

            CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));

            // Process all matching rows in one go
            matchedRows.forEach((IntConsumer) row -> {
                try {
                    kahanSummation.add(intVector.get(row));
                } catch (IllegalStateException e) {
                    System.out.println("Value is null for : " + row);
                }
            });

            // Store final results
            sums.set(0, kahanSummation.value());
            compensations.set(0, kahanSummation.delta());
        }
    }

    public void collectBatch(VectorSchemaRoot root) throws IOException {
        if (root.getFieldVectors().getFirst() instanceof IntVector) {
            IntVector intVector = (IntVector) root.getFieldVectors().getFirst();

            // Get values for bucket 0 (since this is a single-bucket aggregation)
            sums = context.bigArrays().grow(sums, 1);
            compensations = context.bigArrays().grow(compensations, 1);

            CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));

            for (int i = 0; i < root.getRowCount(); i++) {
                try {
                    kahanSummation.add(intVector.get(i));
                } catch (IllegalStateException e) {
                    System.out.println("Value is null for : " + i);
                }
            }

            // Store final results
            sums.set(0, kahanSummation.value());
            compensations.set(0, kahanSummation.delta());
        }
    }

    public void collect(VectorSchemaRoot root, int doc) throws IOException {

        //IntVector intVector = ((IntVector) root.getVector(0));

        // Get values for bucket 0 (since this is a single-bucket aggregation)
        sums = context.bigArrays().grow(sums, 1);
        compensations = context.bigArrays().grow(compensations, 1);

        CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));

        // for (int i = 0; i < root.getRowCount(); i++) {
        try {
            kahanSummation.add(((IntVector) root.getVector(0)).get(doc));
        } catch (IllegalStateException e) {
            System.out.println("Value is null for : " + doc);
        }
        // }

        // Store final results
        sums.set(0, kahanSummation.value());
        compensations.set(0, kahanSummation.delta());
    }

    private LeafBucketCollector getArrowLeafCollector(final LeafBucketCollector sub) {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = context.bigArrays().grow(sums, bucket + 1);
                compensations = context.bigArrays().grow(compensations, bucket + 1);

                // Get the current VectorSchemaRoot from context
                VectorSchemaRoot root = context.getQueryShardContext().getArrowQueryContext().getCurrentBatch();
                if (root != null) {
                    // Get the vector for target_status_code
                    ValueVector vector = root.getVector("target_status_code");
                    if (vector instanceof IntVector) {
                        IntVector intVector = (IntVector) vector;
                        if (!intVector.isNull(doc)) {
                            double value = intVector.get(doc);

                            // Use Kahan summation for accuracy
                            kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                            kahanSummation.add(value);

                            compensations.set(bucket, kahanSummation.delta());
                            sums.set(bucket, kahanSummation.value());
                        }
                    }
                }
            }
        };
    }

    private void precomputeLeafUsingStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));

        StarTreeQueryHelper.precomputeLeafUsingStarTree(
            context,
            valuesSource,
            ctx,
            starTree,
            MetricStat.SUM.getTypeName(),
            value -> kahanSummation.add(NumericUtils.sortableLongToDouble(value)),
            () -> {
                sums.set(0, kahanSummation.value());
                compensations.set(0, kahanSummation.delta());
            }
        );
    }

    /**
     * The parent aggregator invokes this method to get a StarTreeBucketCollector,
     * which exposes collectStarTreeEntry() to be evaluated on filtered star tree entries
     */
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return StarTreeQueryHelper.getStarTreeBucketMetricCollector(
            starTree,
            MetricStat.SUM.getTypeName(),
            valuesSource,
            parentCollector,
            (bucket) -> {
                sums = context.bigArrays().grow(sums, bucket + 1);
                compensations = context.bigArrays().grow(compensations, bucket + 1);
            },
            (bucket, metricValue) -> {
                kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                kahanSummation.add(NumericUtils.sortableLongToDouble(metricValue));
                sums.set(bucket, kahanSummation.value());
                compensations.set(bucket, kahanSummation.delta());
            }
        );
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalSum(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
}
