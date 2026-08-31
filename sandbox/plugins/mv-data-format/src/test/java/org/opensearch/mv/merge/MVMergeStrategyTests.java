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

    public void testDataFusionStateStrategyFoldsInputFiles() throws Exception {
        Path shardData = createTempDir();
        Path firstDirectory = createTempDir();
        Path secondDirectory = createTempDir();
        Path firstFile = Files.writeString(firstDirectory.resolve("first.mv.arrow"), "first");
        Path secondFile = Files.writeString(secondDirectory.resolve("second.mv.arrow"), "second");

        WriterFileSet firstSet = MonoFileWriterSet.of(firstDirectory, 1L, firstFile.getFileName().toString(), 2L);
        WriterFileSet secondSet = MonoFileWriterSet.of(secondDirectory, 2L, secondFile.getFileName().toString(), 3L);
        Segment firstSegment = Segment.builder(1L).addSearchableFiles(MVStateDataFormat.INSTANCE, firstSet).build();
        Segment secondSegment = Segment.builder(2L).addSearchableFiles(MVStateDataFormat.INSTANCE, secondSet).build();
        MergeInput input = MergeInput.builder().segments(List.of(secondSegment, firstSegment)).newWriterGeneration(9L).build();

        ShardPath shardPath = shardPath(shardData);
        AtomicReference<List<String>> mergedInputs = new AtomicReference<>();
        AtomicReference<String> mergedSql = new AtomicReference<>();
        AtomicReference<String> mergedOutput = new AtomicReference<>();

        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "SELECT fold FROM __MV_STATES__",
            (stateFiles, foldSql, outputFile) -> {
                mergedInputs.set(new ArrayList<>(stateFiles));
                mergedSql.set(foldSql);
                mergedOutput.set(outputFile);
                Files.writeString(Path.of(outputFile), "merged");
                return 4L;
            }
        );

        MergeResult result = strategy.mergeMVFiles(input);

        List<String> expectedInputs = new ArrayList<>(List.of(firstFile.toString(), secondFile.toString()));
        expectedInputs.sort(String::compareTo);
        assertEquals(expectedInputs, mergedInputs.get());
        assertEquals("SELECT fold FROM __MV_STATES__", mergedSql.get());
        assertEquals(
            shardPath.getDataPath().resolve(MVStateDataFormat.NAME).resolve(MVConstants.mvFileName(9L)).toString(),
            mergedOutput.get()
        );

        WriterFileSet output = result.getMergedWriterFileSetForDataformat(MVStateDataFormat.INSTANCE);
        assertNotNull(output);
        assertEquals(9L, output.writerGeneration());
        assertEquals(4L, output.numRows());
        assertEquals(1, output.files().size());
    }

    public void testDataFusionStateStrategyRejectsMissingInputs() {
        ShardPath shardPath = shardPath(createTempDir());
        DataFusionMVStateMergeStrategy strategy = new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            "SELECT fold FROM __MV_STATES__"
        );
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(3L).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> strategy.mergeMVFiles(input));
        assertTrue(exception.getMessage().contains("No mv_state files to merge"));
    }

    private ShardPath shardPath(Path root) {
        String indexUUID = "test-index-uuid";
        Path dataPath = root.resolve(indexUUID).resolve("0");
        dataPath.toFile().mkdirs();
        ShardId shardId = new ShardId(new Index("test-index", indexUUID), 0);
        return new ShardPath(false, dataPath, dataPath, shardId);
    }

    public void testNoOpStrategyProducesNoMVOutput() throws Exception {
        MergeInput input = MergeInput.builder().segments(List.of()).newWriterGeneration(4L).build();
        MergeResult result = new NoOpMVMergeStrategy().mergeMVFiles(input);
        assertTrue(result.getMergedWriterFileSet().isEmpty());
    }
}
