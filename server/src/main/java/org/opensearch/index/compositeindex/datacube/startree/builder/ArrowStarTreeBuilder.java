/// *
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
// package org.opensearch.index.compositeindex.datacube.startree.builder;
//
// import org.apache.arrow.memory.BufferAllocator;
// import org.apache.arrow.memory.RootAllocator;
// import org.apache.arrow.vector.BigIntVector;
// import org.apache.arrow.vector.FieldVector;
// import org.apache.arrow.vector.Float8Vector;
// import org.apache.arrow.vector.VectorSchemaRoot;
// import org.apache.arrow.vector.types.FloatingPointPrecision;
// import org.apache.arrow.vector.types.pojo.ArrowType;
// import org.apache.arrow.vector.types.pojo.Field;
// import org.apache.arrow.vector.types.pojo.FieldType;
// import org.apache.arrow.vector.types.pojo.Schema;
// import org.apache.lucene.codecs.DocValuesConsumer;
// import org.apache.lucene.index.OrdinalMap;
// import org.apache.lucene.index.SegmentWriteState;
// import org.apache.lucene.store.IndexOutput;
// import org.apache.lucene.util.LongValues;
// import org.opensearch.common.annotation.ExperimentalApi;
// import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
// import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
// import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
// import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
// import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
// import org.opensearch.index.mapper.MapperService;
// import org.opensearch.index.mapper.NumberFieldMapper;
// import java.io.IOException;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.Iterator;
// import java.util.LinkedHashMap;
// import java.util.List;
// import java.util.Map;
// import java.util.Objects;
// import java.util.concurrent.atomic.AtomicInteger;
/// **
// * Arrow-based single tree builder
// *
// * @opensearch.experimental
// */
// @ExperimentalApi
// public class ArrowStarTreeBuilder extends BaseStarTreeBuilder {
// private final BufferAllocator allocator;
// private final VectorSchemaRoot vectorSchemaRoot;
// private final List<BigIntVector> dimensionVectors;
// private final List<FieldVector> metricVectors;
// private int currentIndex = 0;
// public ArrowStarTreeBuilder(
// IndexOutput metaOut,
// IndexOutput dataOut,
// StarTreeField starTreeField,
// SegmentWriteState segmentWriteState,
// MapperService mapperService
// ) throws IOException {
// super(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
// this.allocator = new RootAllocator();
// List<Field> fields = new ArrayList<>();
// // Add dimension fields
// dimensionVectors = new ArrayList<>(numDimensions);
// for (int i = 0; i < numDimensions; i++) {
// Field field = new Field("dimension_" + i,
// FieldType.nullable(new ArrowType.Int(64, true)), null);
// fields.add(field);
// }
// // Add metric fields
// metricVectors = new ArrayList<>(numMetrics);
// for (int i = 0; i < numMetrics; i++) {
// ValueAggregator<?> aggregator = metricAggregatorInfos.get(i).getValueAggregators();
// ArrowType arrowType;
// if (aggregator.getAggregatedValueType() == NumberFieldMapper.NumberType.LONG) {
// arrowType = new ArrowType.Int(64, true);
// } else {
// arrowType = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
// }
// Field field = new Field("metric_" + i, FieldType.nullable(arrowType), null);
// fields.add(field);
// }
// Schema schema = new Schema(fields);
// this.vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
// // Initialize vectors
// for (int i = 0; i < numDimensions; i++) {
// dimensionVectors.add((BigIntVector) vectorSchemaRoot.getVector("dimension_" + i));
// }
// for (int i = 0; i < numMetrics; i++) {
// metricVectors.add(vectorSchemaRoot.getVector("metric_" + i));
// }
// vectorSchemaRoot.allocateNew();
// }
// @Override
// public void appendStarTreeDocument(StarTreeDocument starTreeDocument) {
// // Ensure capacity
// if (currentIndex >= vectorSchemaRoot.getRowCount()) {
// vectorSchemaRoot.setRowCount(Math.max(1000, currentIndex * 2));
// }
// // Set dimension values
// for (int i = 0; i < numDimensions; i++) {
// Long value = starTreeDocument.dimensions[i];
// if (value == null) {
// dimensionVectors.get(i).setNull(currentIndex);
// } else {
// dimensionVectors.get(i).set(currentIndex, value);
// }
// }
// // Set metric values
// for (int i = 0; i < numMetrics; i++) {
// FieldVector vector = metricVectors.get(i);
// Object value = starTreeDocument.metrics[i];
// if (value == null) {
// vector.setNull(currentIndex);
// } else if (vector instanceof BigIntVector) {
// ((BigIntVector) vector).set(currentIndex, ((Number) value).longValue());
// } else if (vector instanceof Float8Vector) {
// ((Float8Vector) vector).set(currentIndex, ((Number) value).doubleValue());
// }
// }
// currentIndex++;
// }
// @Override
// public StarTreeDocument getStarTreeDocument(int docId) {
// Long[] dimensions = new Long[numDimensions];
// Object[] metrics = new Object[numMetrics];
// // Get dimension values
// for (int i = 0; i < numDimensions; i++) {
// if (dimensionVectors.get(i).isNull(docId)) {
// dimensions[i] = null;
// } else {
// dimensions[i] = dimensionVectors.get(i).get(docId);
// }
// }
// // Get metric values
// for (int i = 0; i < numMetrics; i++) {
// FieldVector vector = metricVectors.get(i);
// if (vector.isNull(docId)) {
// metrics[i] = null;
// } else if (vector instanceof BigIntVector) {
// metrics[i] = ((BigIntVector) vector).get(docId);
// } else if (vector instanceof Float8Vector) {
// metrics[i] = ((Float8Vector) vector).get(docId);
// }
// }
// return new StarTreeDocument(dimensions, metrics);
// }
// @Override
// public List<StarTreeDocument> getStarTreeDocuments() {
// List<StarTreeDocument> documents = new ArrayList<>(currentIndex);
// for (int i = 0; i < currentIndex; i++) {
// documents.add(getStarTreeDocument(i));
// }
// return documents;
// }
// @Override
// public Long getDimensionValue(int docId, int dimensionId) {
// if (dimensionVectors.get(dimensionId).isNull(docId)) {
// return null;
// }
// return dimensionVectors.get(dimensionId).get(docId);
// }
// @Override
// public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
// SequentialDocValuesIterator[] dimensionReaders,
// List<SequentialDocValuesIterator> metricReaders
// ) throws IOException {
// StarTreeDocument[] starTreeDocuments = new StarTreeDocument[totalSegmentDocs];
// for (int currentDocId = 0; currentDocId < totalSegmentDocs; currentDocId++) {
// starTreeDocuments[currentDocId] = getSegmentStarTreeDocument(currentDocId, dimensionReaders, metricReaders);
// }
// return sortAndAggregateStarTreeDocuments(starTreeDocuments, false);
// }
// @Override
// public void build(
// List<StarTreeValues> starTreeValuesSubs,
// AtomicInteger fieldNumberAcrossStarTrees,
// DocValuesConsumer starTreeDocValuesConsumer
// ) throws IOException {
// build(mergeStarTrees(starTreeValuesSubs), fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
// }
// @Override
// Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
// this.isMerge = true;
// return sortAndAggregateStarTreeDocuments(getSegmentsStarTreeDocuments(starTreeValuesSubs), true);
// }
// StarTreeDocument[] getSegmentsStarTreeDocuments(List<StarTreeValues> starTreeValuesSubs) throws IOException {
// List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
// Map<String, OrdinalMap> ordinalMaps = getOrdinalMaps(starTreeValuesSubs);
// int seg = 0;
// for (StarTreeValues starTreeValues : starTreeValuesSubs) {
// SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[numDimensions];
// List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
// AtomicInteger numSegmentDocs = new AtomicInteger();
// setReadersAndNumSegmentDocsDuringMerge(dimensionReaders, metricReaders, numSegmentDocs, starTreeValues);
// int currentDocId = 0;
// Map<String, LongValues> longValuesMap = new LinkedHashMap<>();
// for (Map.Entry<String, OrdinalMap> entry : ordinalMaps.entrySet()) {
// longValuesMap.put(entry.getKey(), entry.getValue().getGlobalOrds(seg));
// }
// while (currentDocId < numSegmentDocs.get()) {
// starTreeDocuments.add(getStarTreeDocument(currentDocId, dimensionReaders, metricReaders, longValuesMap));
// currentDocId++;
// }
// seg++;
// }
// return starTreeDocuments.toArray(new StarTreeDocument[0]);
// }
// @Override
// public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId) {
// int numDocs = endDocId - startDocId;
// StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
// for (int i = 0; i < numDocs; i++) {
// starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
// }
// sortStarTreeDocumentsFromDimensionId(starTreeDocuments, dimensionId + 1);
// return new Iterator<StarTreeDocument>() {
// boolean hasNext = true;
// StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
// int docId = 1;
// private boolean hasSameDimensions(StarTreeDocument doc1, StarTreeDocument doc2) {
// for (int i = dimensionId + 1; i < numDimensions; i++) {
// if (!Objects.equals(doc1.dimensions[i], doc2.dimensions[i])) {
// return false;
// }
// }
// return true;
// }
// @Override
// public boolean hasNext() {
// return hasNext;
// }
// @Override
// public StarTreeDocument next() {
// StarTreeDocument next = reduceStarTreeDocuments(null, currentStarTreeDocument);
// next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
// while (docId < numDocs) {
// StarTreeDocument starTreeDocument = starTreeDocuments[docId];
// docId++;
// if (!hasSameDimensions(starTreeDocument, currentStarTreeDocument)) {
// currentStarTreeDocument = starTreeDocument;
// return next;
// } else {
// next = reduceStarTreeDocuments(next, starTreeDocument);
// }
// }
// hasNext = false;
// return next;
// }
// };
// }
// private Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {
// sortStarTreeDocumentsFromDimensionId(starTreeDocuments, 0);
// return mergeStarTreeDocuments(starTreeDocuments, isMerge);
// }
// private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {
// return new Iterator<>() {
// boolean hasNext = true;
// StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
// int docId = 1;
// @Override
// public boolean hasNext() {
// return hasNext;
// }
// @Override
// public StarTreeDocument next() {
// StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentStarTreeDocument, isMerge);
// while (docId < starTreeDocuments.length) {
// StarTreeDocument starTreeDocument = starTreeDocuments[docId];
// docId++;
// if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
// currentStarTreeDocument = starTreeDocument;
// return next;
// } else {
// next = reduceSegmentStarTreeDocuments(next, starTreeDocument, isMerge);
// }
// }
// hasNext = false;
// return next;
// }
// };
// }
// private void sortStarTreeDocumentsFromDimensionId(StarTreeDocument[] starTreeDocuments, int dimensionId) {
// Arrays.sort(starTreeDocuments, (o1, o2) -> {
// for (int i = dimensionId; i < numDimensions; i++) {
// if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
// if (o1.dimensions[i] == null && o2.dimensions[i] == null) {
// return 0;
// }
// if (o1.dimensions[i] == null) {
// return 1;
// }
// if (o2.dimensions[i] == null) {
// return -1;
// }
// return Long.compare(o1.dimensions[i], o2.dimensions[i]);
// }
// }
// return 0;
// });
// }
// @Override
// public void close() throws IOException {
// super.close();
// vectorSchemaRoot.close();
// allocator.close();
// }
// }
