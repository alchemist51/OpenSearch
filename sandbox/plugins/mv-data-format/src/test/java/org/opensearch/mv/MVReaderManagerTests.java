/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.mv.MVReaderManager.MVReader;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MVReaderManagerTests extends OpenSearchTestCase {

    private static WriterFileSet stateFileSet(long generation) {
        return WriterFileSet.builder()
            .directory(Path.of("/tmp/shard0/materialized_view"))
            .writerGeneration(generation)
            .addFile("mv_state_" + generation + ".parquet")
            .addNumRows(randomLongBetween(1, 1000))
            .build();
    }

    public void testCoveredGenerations() {
        MVReader reader = new MVReader(List.of(stateFileSet(1), stateFileSet(3), stateFileSet(7)));
        assertEquals(Set.of(1L, 3L, 7L), reader.coveredGenerations());
    }

    public void testCoveredGenerationsEmpty() {
        MVReader reader = new MVReader(List.of());
        assertTrue(reader.coveredGenerations().isEmpty());
        assertFalse(reader.covers(randomNonNegativeLong()));
    }

    public void testCovers() {
        MVReader reader = new MVReader(List.of(stateFileSet(2), stateFileSet(5)));
        assertTrue(reader.covers(2));
        assertTrue(reader.covers(5));
        assertFalse(reader.covers(3));
    }

    public void testStateFilesByGeneration() {
        WriterFileSet gen1 = stateFileSet(1);
        WriterFileSet gen4 = stateFileSet(4);
        MVReader reader = new MVReader(List.of(gen1, gen4));
        Map<Long, WriterFileSet> byGen = reader.stateFilesByGeneration();
        assertEquals(2, byGen.size());
        assertSame(gen1, byGen.get(1L));
        assertSame(gen4, byGen.get(4L));
    }

    public void testCoverageSplitAgainstPrimaryGenerations() {
        // The read-path split: primary (parquet) generations minus covered = uncovered.
        MVReader reader = new MVReader(List.of(stateFileSet(1), stateFileSet(2)));
        Set<Long> primaryGenerations = Set.of(1L, 2L, 3L);
        Set<Long> covered = reader.coveredGenerations();
        assertEquals(Set.of(1L, 2L), covered);
        assertEquals(
            Set.of(3L),
            primaryGenerations.stream().filter(g -> covered.contains(g) == false).collect(java.util.stream.Collectors.toSet())
        );
    }
}
