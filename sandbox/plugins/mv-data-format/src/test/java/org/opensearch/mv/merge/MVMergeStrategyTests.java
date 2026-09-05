/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.merge;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the generic {@link DataFusionMVStateMergeStrategy}: the
 * strategy must collect input files, resolve output naming/generation, invoke
 * the injected merger with the runtime pointer and ordering-contract flags
 * (ASC + NULLS FIRST), and propagate seq ranges (min/max when all known,
 * UNKNOWN otherwise). Native execution is covered by the Rust sorted_merge
 * tests; ITs cover the end-to-end engine path.
 */
public class MVMergeStrategyTests extends OpenSearchTestCase {

    private static final long RUNTIME_PTR = 0xCAFEL;

    private ShardPath shardPath;
    private Path dataDir;

    /** Captured arguments from the injected merger. */
    private record CapturedCall(
        long runtimePtr,
        List<String> inputFiles,
        String[] sortColumns,
        boolean[] descending,
        boolean[] nullsFirst,
        String outputFile
    ) {}

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Path shardDir = createTempDir().resolve("indices").resolve("uuid").resolve("0");
        Files.createDirectories(shardDir);
        dataDir = shardDir;
        shardPath = new ShardPath(false, shardDir, shardDir, new ShardId("mv-test", "uuid", 0));
    }

    private Path stateDir() throws IOException {
        Path dir = shardPath.getDataPath().resolve(MVStateDataFormat.INSTANCE.name());
        Files.createDirectories(dir);
        return dir;
    }

    private WriterFileSet fileSet(Path dir, long gen, String name, long minSeq, long maxSeq) throws IOException {
        Files.write(dir.resolve(name), new byte[] { 1, 2, 3 });
        return MonoFileWriterSet.of(dir, gen, name, 10L, 0L, minSeq, maxSeq);
    }

    private MergeInput inputOf(long nextGen, WriterFileSet... sets) {
        List<Segment> segments = new ArrayList<>();
        for (WriterFileSet ws : sets) {
            segments.add(Segment.builder(ws.writerGeneration()).addSearchableFiles(MVStateDataFormat.INSTANCE, ws).build());
        }
        return MergeInput.builder().segments(segments).newWriterGeneration(nextGen).build();
    }

    /** Strategy whose merger records its arguments and whose sort-name read is stubbed. */
    private DataFusionMVStateMergeStrategy capturing(AtomicReference<CapturedCall> captured, long rows) {
        return new DataFusionMVStateMergeStrategy(
            MVStateDataFormat.INSTANCE,
            shardPath,
            2,
            RUNTIME_PTR,
            (ptr, inputs, cols, desc, nulls, out) -> {
                captured.set(new CapturedCall(ptr, inputs, cols, desc, nulls, out));
                return rows;
            },
            (file, count) -> List.of("k1", "k2")
        );
    }

    public void testRejectsNonPositiveKeyCountAndNullRuntime() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataFusionMVStateMergeStrategy(MVStateDataFormat.INSTANCE, shardPath, 0, RUNTIME_PTR)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> new DataFusionMVStateMergeStrategy(MVStateDataFormat.INSTANCE, shardPath, 2, 0L)
        );
    }

    public void testRejectsEmptyInput() {
        AtomicReference<CapturedCall> captured = new AtomicReference<>();
        DataFusionMVStateMergeStrategy strategy = capturing(captured, 1L);
        expectThrows(IllegalArgumentException.class, () -> strategy.mergeMVFiles(inputOf(9L)));
        assertNull("merger must not be invoked for empty input", captured.get());
    }

    public void testMissingInputFileFailsClosed() throws Exception {
        Path dir = stateDir();
        WriterFileSet present = fileSet(dir, 1L, "_mv_poc_1.mv.parquet", 0L, 99L);
        WriterFileSet ghost = MonoFileWriterSet.of(dir, 2L, "_mv_poc_2.mv.parquet", 10L, 0L, 100L, 199L);
        AtomicReference<CapturedCall> captured = new AtomicReference<>();
        DataFusionMVStateMergeStrategy strategy = capturing(captured, 1L);
        expectThrows(IOException.class, () -> strategy.mergeMVFiles(inputOf(9L, present, ghost)));
        assertNull(captured.get());
    }

    public void testSeqRangePropagationAllKnown() throws Exception {
        Path dir = stateDir();
        WriterFileSet a = fileSet(dir, 1L, "_mv_poc_1.mv.parquet", 100L, 199L);
        WriterFileSet b = fileSet(dir, 2L, "_mv_poc_2.mv.parquet", 0L, 99L);
        WriterFileSet c = fileSet(dir, 3L, "_mv_poc_3.mv.parquet", 200L, 250L);

        AtomicReference<CapturedCall> captured = new AtomicReference<>();
        MergeResult result = capturing(captured, 42L).mergeMVFiles(inputOf(9L, a, b, c));

        WriterFileSet merged = result.getMergedWriterFileSet().values().iterator().next();
        assertEquals(0L, merged.minSeqNo());
        assertEquals(250L, merged.maxSeqNo());
        assertEquals(9L, merged.writerGeneration());

        CapturedCall call = captured.get();
        assertEquals(RUNTIME_PTR, call.runtimePtr());
        assertEquals(3, call.inputFiles().size());
        // Ordering contract: ASC + NULLS FIRST on every sort column.
        for (boolean d : call.descending()) {
            assertFalse(d);
        }
        for (boolean n : call.nullsFirst()) {
            assertTrue(n);
        }
        assertTrue(call.outputFile().endsWith("_mv_poc_9.mv.parquet"));
    }

    public void testSeqRangeUnknownPropagatesUnknown() throws Exception {
        Path dir = stateDir();
        WriterFileSet known = fileSet(dir, 1L, "_mv_poc_1.mv.parquet", 0L, 99L);
        WriterFileSet unknown = fileSet(dir, 2L, "_mv_poc_2.mv.parquet", WriterFileSet.UNKNOWN_SEQ_NO, WriterFileSet.UNKNOWN_SEQ_NO);

        AtomicReference<CapturedCall> captured = new AtomicReference<>();
        MergeResult result = capturing(captured, 42L).mergeMVFiles(inputOf(9L, known, unknown));

        WriterFileSet merged = result.getMergedWriterFileSet().values().iterator().next();
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, merged.minSeqNo());
        assertEquals(WriterFileSet.UNKNOWN_SEQ_NO, merged.maxSeqNo());
    }
}
