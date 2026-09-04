/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.merge;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class MVMergeStrategyTests extends OpenSearchTestCase {

    public void testExecutorDelegatesToSelectedStrategy() throws Exception {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(7L).build();
        MergeResult expected = new MergeResult(Map.of());
        AtomicReference<MergeInput> actual = new AtomicReference<>();
        MVMergeExecutor executor = new MVMergeExecutor(mergeInput -> {
            actual.set(mergeInput);
            return expected;
        });

        assertSame(expected, executor.merge(input));
        assertSame(input, actual.get());
    }

    public void testDataFusionStateStrategyFoldsInputFilesViaStreaming() throws Exception {
        Path shardData = createTempDir();
        Path firstDirectory = createTempDir();
        Path secondDirectory = createTempDir();
        Path firstFile = Files.writeString(firstDirectory.resolve("first.mv.parquet"), "first");
        Path secondFile = Files.writeString(secondDirectory.resolve("second.mv.parquet"), "second");

        WriterFileSet firstSet = MonoFileWriterSet.of(firstDirectory, 1L, firstFile.getFileName().toString(), 2L);
        WriterFileSet secondSet = MonoFileWriterSet.of(secondDirectory, 2L, secondFile.getFileName().toString(), 3L);
        Segment firstSegment = Segment.builder(1L).addSearchableFiles(MVStateDataFormat.INSTANCE, firstSet).build();
        Segment secondSegment = Segment.builder(2L).addSearchableFiles(MVStateDataFormat.INSTANCE, secondSet).build();
        MergeInput input = MergeInput.builder().segments(List.of(secondSegment, firstSegment)).newWriterGeneration(9L).build();

        ShardPath shardPath = shardPath(shardData);
        AtomicReference<List<String>> mergedInputs = new AtomicReference<>();
        AtomicReference<String> mergedOutput = new AtomicReference<>();
        AtomicReference<org.opensearch.mv.MVCompiledDefinition.MergeCallParams> capturedParams = new AtomicReference<>();

        org.opensearch.mv.MVCompiledDefinition compiledDef = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(org.opensearch.mv.GroupKey.of("k", org.opensearch.mv.GroupKey.ColumnType.LONG)),
            java.util.List.of(org.opensearch.mv.AggregateSpec.count("cnt"), org.opensearch.mv.AggregateSpec.sum("v", "sum_v"))
        );

        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "unused_fold_sql",
            compiledDef,
            (stateFiles, outputFile, params) -> {
                mergedInputs.set(new ArrayList<>(stateFiles));
                mergedOutput.set(outputFile);
                capturedParams.set(params);
                try { Files.writeString(Path.of(outputFile), "merged"); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                return 4L;
            }
        );

        MergeResult result = strategy.mergeMVFiles(input);

        List<String> expectedInputs = new ArrayList<>(List.of(firstFile.toString(), secondFile.toString()));
        expectedInputs.sort(String::compareTo);
        assertEquals(expectedInputs, mergedInputs.get());
        assertEquals(
            shardPath.getDataPath().resolve(MVStateDataFormat.NAME).resolve(MVConstants.mvFileName(9L)).toString(),
            mergedOutput.get()
        );

        // Verify the MergeCallParams were correctly derived
        assertNotNull(capturedParams.get());
        assertEquals("0:k:0:0", capturedParams.get().orderingIdentity());
        assertArrayEquals(new int[] { 0 }, capturedParams.get().orderingIndices());
        assertArrayEquals(new String[] { "count(*)[count]", "sum(mv_input.v)[sum]" }, capturedParams.get().aggColumnNames());

        WriterFileSet output = result.getMergedWriterFileSetForDataformat(MVStateDataFormat.INSTANCE);
        assertNotNull(output);
        assertEquals(9L, output.writerGeneration());
        assertEquals(4L, output.numRows());
        assertEquals(1, output.files().size());
    }

    public void testDataFusionStateStrategyRejectsMissingInputs() {
        ShardPath shardPath = shardPath(createTempDir());
        org.opensearch.mv.MVCompiledDefinition compiledDef = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(org.opensearch.mv.GroupKey.of("k", org.opensearch.mv.GroupKey.ColumnType.LONG)),
            java.util.List.of(org.opensearch.mv.AggregateSpec.count("cnt"))
        );
        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "SELECT fold FROM __MV_STATES__",
            compiledDef,
            0L
        );
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(3L).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> strategy.mergeMVFiles(input));
        assertTrue(exception.getMessage().contains("No mv_state files to merge"));
    }

    private ShardPath shardPath(Path root) {
        String indexUUID = "test-index-uuid";
        Path dataPath = root.resolve(indexUUID).resolve("0");
        try {
            java.nio.file.Files.createDirectories(dataPath);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
        ShardId shardId = new ShardId(new Index("test-index", indexUUID), 0);
        return new ShardPath(false, dataPath, dataPath, shardId);
    }

    public void testNoOpStrategyProducesNoMVOutput() throws Exception {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(4L).build();
        MergeResult result = new NoOpMVMergeStrategy().mergeMVFiles(input);
        assertTrue(result.getMergedWriterFileSet().isEmpty());
    }

    /**
     * Stage 4: Verifies that the streaming merge path receives correct FFI
     * parameters including ordering contract and per-column fold ops.
     */
    public void testDataFusionStateStrategyUsesStreamingWithCorrectParams() throws Exception {
        Path shardData = createTempDir();
        Path firstDirectory = createTempDir();
        Path firstFile = Files.writeString(firstDirectory.resolve("first.mv.parquet"), "first");

        WriterFileSet firstSet = MonoFileWriterSet.of(firstDirectory, 1L, firstFile.getFileName().toString(), 2L);
        Segment firstSegment = Segment.builder(1L).addSearchableFiles(MVStateDataFormat.INSTANCE, firstSet).build();
        MergeInput input = MergeInput.builder().segments(List.of(firstSegment)).newWriterGeneration(5L).build();

        ShardPath shardPath = shardPath(shardData);

        org.opensearch.mv.MVCompiledDefinition compiledDef = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(
                org.opensearch.mv.GroupKey.of("region", org.opensearch.mv.GroupKey.ColumnType.LONG),
                org.opensearch.mv.GroupKey.of("os", org.opensearch.mv.GroupKey.ColumnType.LONG)
            ),
            java.util.List.of(
                org.opensearch.mv.AggregateSpec.count("cnt"),
                org.opensearch.mv.AggregateSpec.sum("val", "sum_val"),
                org.opensearch.mv.AggregateSpec.min("val", "min_val"),
                org.opensearch.mv.AggregateSpec.max("val", "max_val")
            )
        );

        AtomicReference<org.opensearch.mv.MVCompiledDefinition.MergeCallParams> capturedParams = new AtomicReference<>();

        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "unused",
            compiledDef,
            (stateFiles, outputFile, params) -> {
                capturedParams.set(params);
                try { Files.writeString(Path.of(outputFile), "merged"); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                return 10L;
            }
        );

        MergeResult result = strategy.mergeMVFiles(input);
        assertNotNull(result.getMergedWriterFileSetForDataformat(MVStateDataFormat.INSTANCE));

        // Verify all FFI parameters
        org.opensearch.mv.MVCompiledDefinition.MergeCallParams params = capturedParams.get();
        assertNotNull(params);

        // 2 ordering keys
        assertArrayEquals(new int[] { 0, 1 }, params.orderingIndices());
        assertTrue(params.orderingAsc()[0]);
        assertTrue(params.orderingAsc()[1]);
        assertTrue(params.orderingNullsFirst()[0]);
        assertTrue(params.orderingNullsFirst()[1]);

        // 6 total fold ops: 2 GROUP_KEY + 4 aggregate
        assertEquals(6, params.foldOps().length);
        assertEquals(0, params.foldOps()[0]); // GROUP_KEY (region)
        assertEquals(0, params.foldOps()[1]); // GROUP_KEY (os)
        assertEquals(1, params.foldOps()[2]); // cnt → SUM fold
        assertEquals(1, params.foldOps()[3]); // sum_val → SUM fold
        assertEquals(2, params.foldOps()[4]); // min_val → MIN fold
        assertEquals(3, params.foldOps()[5]); // max_val → MAX fold

        // 4 agg column names (physical DataFusion names)
        assertArrayEquals(
            new String[] {
                "count(*)[count]",
                "sum(mv_input.val)[sum]",
                "min(mv_input.val)[value]",
                "max(mv_input.val)[value]"
            },
            params.aggColumnNames()
        );

        // Ordering identity
        assertEquals("0:region:0:0;1:os:0:0", params.orderingIdentity());
    }

    /**
     * Stage 4: Verifies that the legacy constructor (no compiled definition)
     * throws since the legacy SQL merge path has been removed.
     */
    @SuppressWarnings("removal")
    public void testLegacyConstructorThrows() {
        ShardPath shardPath = shardPath(createTempDir());
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataFusionMVStateMergeStrategy(MVStateDataFormat.INSTANCE, shardPath, "SELECT fold FROM table")
        );
    }

    /**
     * Stage 4: Verifies that passing null compiled definition to the primary
     * constructor throws since the streaming merge requires typed metadata.
     */
    public void testNullCompiledDefinitionThrows() {
        ShardPath shardPath = shardPath(createTempDir());
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataFusionMVStateMergeStrategy(MVStateDataFormat.INSTANCE, shardPath, "sql", null, 0L)
        );
    }

    /**
     * Stage 4: Verifies that the AggregateFFIMetadata correctly maps accumulator
     * types to the expected values. SUM and COUNT both use ACC_SUM (0),
     * MIN uses ACC_MIN (1), MAX uses ACC_MAX (2). The merge strategy uses
     * MergeCallParams which adds 1 to map these to Rust FoldOp wire values:
     * SUM→1, MIN→2, MAX→3.
     */
    public void testAggregateFFIMetadataFoldOpDerivation() {
        org.opensearch.mv.MVCompiledDefinition def = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(
                org.opensearch.mv.GroupKey.of("region", org.opensearch.mv.GroupKey.ColumnType.LONG),
                org.opensearch.mv.GroupKey.of("os", org.opensearch.mv.GroupKey.ColumnType.LONG)
            ),
            java.util.List.of(
                org.opensearch.mv.AggregateSpec.count("cnt"),
                org.opensearch.mv.AggregateSpec.sum("val", "sum_val"),
                org.opensearch.mv.AggregateSpec.min("val", "min_val"),
                org.opensearch.mv.AggregateSpec.max("val", "max_val")
            )
        );

        org.opensearch.mv.MVCompiledDefinition.AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();
        assertEquals(4, aggMeta.length());

        // COUNT → ACC_SUM (folds via addition, same as SUM)
        assertEquals(org.opensearch.mv.MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[0]);
        // SUM → ACC_SUM
        assertEquals(org.opensearch.mv.MVCompiledDefinition.AggregateFFIMetadata.ACC_SUM, aggMeta.accumulatorTypes()[1]);
        // MIN → ACC_MIN
        assertEquals(org.opensearch.mv.MVCompiledDefinition.AggregateFFIMetadata.ACC_MIN, aggMeta.accumulatorTypes()[2]);
        // MAX → ACC_MAX
        assertEquals(org.opensearch.mv.MVCompiledDefinition.AggregateFFIMetadata.ACC_MAX, aggMeta.accumulatorTypes()[3]);

        // Verify state column names
        assertEquals("cnt", aggMeta.stateColumnNames()[0]);
        assertEquals("sum_val", aggMeta.stateColumnNames()[1]);
        assertEquals("min_val", aggMeta.stateColumnNames()[2]);
        assertEquals("max_val", aggMeta.stateColumnNames()[3]);

        // Verify fold-op derivation through MergeCallParams (the actual code path)
        org.opensearch.mv.MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
        // 2 group keys → GROUP_KEY (0)
        assertEquals(0, params.foldOps()[0]);
        assertEquals(0, params.foldOps()[1]);
        // cnt (COUNT→ACC_SUM=0) → SUM fold (1)
        assertEquals(1, params.foldOps()[2]);
        // sum_val (SUM→ACC_SUM=0) → SUM fold (1)
        assertEquals(1, params.foldOps()[3]);
        // min_val (MIN→ACC_MIN=1) → MIN fold (2)
        assertEquals(2, params.foldOps()[4]);
        // max_val (MAX→ACC_MAX=2) → MAX fold (3)
        assertEquals(3, params.foldOps()[5]);
    }

    /**
     * Stage 4: Verifies ordering identity derivation from a multi-key
     * compiled definition.
     */
    public void testOrderingIdentityDerivation() {
        org.opensearch.mv.MVCompiledDefinition def = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(
                org.opensearch.mv.GroupKey.of("region", org.opensearch.mv.GroupKey.ColumnType.LONG),
                org.opensearch.mv.GroupKey.of("os", org.opensearch.mv.GroupKey.ColumnType.LONG)
            ),
            java.util.List.of(org.opensearch.mv.AggregateSpec.count("cnt"))
        );

        org.opensearch.mv.MVGroupByOrdering ordering = def.groupByOrdering();
        String identity = ordering.orderingIdentity();
        // Format: "idx:col:dir:null;..."
        assertEquals("0:region:0:0;1:os:0:0", identity);
    }

    /**
     * Stage 4: Verifies that the AggregateFFIMetadata correctly maps
     * SUM/MIN/MAX/COUNT accumulator types through MergeCallParams.
     */
    public void testStreamingMergeFoldOpsUsesCorrectAccumulatorTypes() {
        org.opensearch.mv.MVCompiledDefinition def = org.opensearch.mv.MVCompiledDefinition.forCountSumMinMaxAvg(
            "GroupCol", "sumF", "minF", "maxF", null
        );

        org.opensearch.mv.MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();
        // GROUP_KEY, SUM-fold(1), SUM-fold(1), MIN-fold(2), MAX-fold(3)
        assertEquals(0, params.foldOps()[0]); // GroupCol
        assertEquals(1, params.foldOps()[1]); // cnt (COUNT → SUM-fold)
        assertEquals(1, params.foldOps()[2]); // sum_sumF (SUM → SUM-fold)
        assertEquals(2, params.foldOps()[3]); // min_minF (MIN → MIN-fold)
        assertEquals(3, params.foldOps()[4]); // max_maxF (MAX → MAX-fold)
    }

    /**
     * Stage 4: Verifies that the MergeFFIBundle collects the correct
     * ordering identity for merge validation.
     */
    public void testMergeFFIBundleOrderingIdentity() {
        org.opensearch.mv.MVCompiledDefinition def = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(
                org.opensearch.mv.GroupKey.of("k0", org.opensearch.mv.GroupKey.ColumnType.LONG),
                org.opensearch.mv.GroupKey.of("k1", org.opensearch.mv.GroupKey.ColumnType.KEYWORD)
            ),
            java.util.List.of(org.opensearch.mv.AggregateSpec.count("cnt"))
        );

        org.opensearch.mv.MVCompiledDefinition.MergeFFIBundle bundle = def.mergeFFIBundle();
        assertEquals("0:k0:0:0;1:k1:0:0", bundle.orderingIdentity());
        assertEquals(2, bundle.ordering().length());
        assertEquals(1, bundle.aggregates().length());
        assertEquals(3, bundle.totalStateColumns());
    }

    /**
     * Stage 4: Verifies MergeCallParams produces correct fold-op bytes and
     * can be passed directly to MVNativeBridge.mergeStateStreams via the
     * new convenience overload.
     */
    public void testMergeCallParamsReadyForFFI() {
        org.opensearch.mv.MVCompiledDefinition def = org.opensearch.mv.MVCompiledDefinition.of(
            java.util.List.of(
                org.opensearch.mv.GroupKey.of("region", org.opensearch.mv.GroupKey.ColumnType.LONG),
                org.opensearch.mv.GroupKey.of("os", org.opensearch.mv.GroupKey.ColumnType.LONG)
            ),
            java.util.List.of(
                org.opensearch.mv.AggregateSpec.count("cnt"),
                org.opensearch.mv.AggregateSpec.sum("val", "sum_val"),
                org.opensearch.mv.AggregateSpec.min("val", "min_val"),
                org.opensearch.mv.AggregateSpec.max("val", "max_val")
            )
        );

        org.opensearch.mv.MVCompiledDefinition.MergeCallParams params = def.buildMergeCallParams();

        // 2 ordering keys
        assertEquals(2, params.orderingIndices().length);
        assertEquals(0, params.orderingIndices()[0]);
        assertEquals(1, params.orderingIndices()[1]);

        // 6 total fold ops: 2 GROUP_KEY + 4 aggregate
        assertEquals(6, params.foldOps().length);
        assertEquals(0, params.foldOps()[0]); // GROUP_KEY
        assertEquals(0, params.foldOps()[1]); // GROUP_KEY
        assertEquals(1, params.foldOps()[2]); // cnt → SUM fold
        assertEquals(1, params.foldOps()[3]); // sum_val → SUM fold
        assertEquals(2, params.foldOps()[4]); // min_val → MIN fold
        assertEquals(3, params.foldOps()[5]); // max_val → MAX fold

        // 4 agg column names (physical DataFusion names)
        assertEquals(4, params.aggColumnNames().length);
        assertEquals("count(*)[count]", params.aggColumnNames()[0]);
        assertEquals("sum(mv_input.val)[sum]", params.aggColumnNames()[1]);
        assertEquals("min(mv_input.val)[value]", params.aggColumnNames()[2]);
        assertEquals("max(mv_input.val)[value]", params.aggColumnNames()[3]);

        // Ordering identity
        assertEquals("0:region:0:0;1:os:0:0", params.orderingIdentity());
    }

    /**
     * Stage 4: End-to-end wiring test — verifies the complete path from
     * compiled definition through DataFusionMVStateMergeStrategy through
     * to the StreamingMerger with a heavy definition (L3: 10 group keys,
     * 30 metrics × 4 quad = 130 total columns).
     */
    public void testEndToEndStreamingMergeWiringHeavyL3() throws Exception {
        Path shardData = createTempDir();
        Path dir1 = createTempDir();
        Path dir2 = createTempDir();
        Path dir3 = createTempDir();
        Path file1 = Files.writeString(dir1.resolve("a.mv.parquet"), "data1");
        Path file2 = Files.writeString(dir2.resolve("b.mv.parquet"), "data2");
        Path file3 = Files.writeString(dir3.resolve("c.mv.parquet"), "data3");

        WriterFileSet set1 = MonoFileWriterSet.of(dir1, 1L, file1.getFileName().toString(), 10L);
        WriterFileSet set2 = MonoFileWriterSet.of(dir2, 2L, file2.getFileName().toString(), 20L);
        WriterFileSet set3 = MonoFileWriterSet.of(dir3, 3L, file3.getFileName().toString(), 30L);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(MVStateDataFormat.INSTANCE, set1).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(MVStateDataFormat.INSTANCE, set2).build();
        Segment seg3 = Segment.builder(3L).addSearchableFiles(MVStateDataFormat.INSTANCE, set3).build();
        MergeInput input = MergeInput.builder().segments(List.of(seg1, seg2, seg3)).newWriterGeneration(10L).build();

        ShardPath shardPath = shardPath(shardData);
        org.opensearch.mv.MVCompiledDefinition heavyL3 = org.opensearch.mv.MVCompiledDefinition.heavyL3();
        AtomicReference<org.opensearch.mv.MVCompiledDefinition.MergeCallParams> capturedParams = new AtomicReference<>();
        AtomicReference<List<String>> capturedInputs = new AtomicReference<>();

        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "unused",
            heavyL3,
            (stateFiles, outputFile, params) -> {
                capturedInputs.set(new ArrayList<>(stateFiles));
                capturedParams.set(params);
                try { Files.writeString(Path.of(outputFile), "merged"); } catch (java.io.IOException e) { throw new java.io.UncheckedIOException(e); }
                return 100L;
            }
        );

        MergeResult result = strategy.mergeMVFiles(input);
        assertNotNull(result);

        // Verify 3 input files were passed through
        assertEquals(3, capturedInputs.get().size());

        // Verify L3 parameters: 10 group keys, 30 metrics × 4 = 120 agg columns
        org.opensearch.mv.MVCompiledDefinition.MergeCallParams params = capturedParams.get();
        assertEquals(10, params.orderingIndices().length);
        assertEquals(130, params.foldOps().length); // 10 GK + 120 agg
        assertEquals(120, params.aggColumnNames().length);

        // First 10 should be GROUP_KEY (0)
        for (int i = 0; i < 10; i++) {
            assertEquals("fold op " + i + " should be GROUP_KEY", 0, params.foldOps()[i]);
        }
        // Rest should be non-zero fold ops (SUM=1, MIN=2, MAX=3)
        for (int i = 10; i < 130; i++) {
            assertTrue("fold op " + i + " should be > 0", params.foldOps()[i] > 0);
        }

        // Verify ordering identity matches the definition
        assertEquals(heavyL3.groupByOrdering().orderingIdentity(), params.orderingIdentity());
    }
}
