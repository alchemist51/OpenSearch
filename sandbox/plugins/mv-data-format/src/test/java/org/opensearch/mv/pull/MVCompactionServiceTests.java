/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MVCompactionServiceTests extends OpenSearchTestCase {

    private CatalogSnapshot catalogOf(List<Segment> segments) {
        return CatalogSnapshotManager.createInitialSnapshot(1L, 1L, 0L, segments, 0L, Map.of());
    }

    /**
     * When the mv_state segment count is at or below the threshold,
     * selectCandidates returns an empty list.
     */
    public void testSelectCandidatesBelowThreshold() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = createSegments(dir, 5);
        CatalogSnapshot catalog = catalogOf(segments);

        MVCompactionService service = new MVCompactionService(null, null);
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 8);
        assertEquals("should not compact when below threshold", 0, candidates.size());
    }

    /**
     * When the mv_state segment count exceeds the threshold, selectCandidates
     * returns the oldest segments, keeping the last 2 untouched.
     */
    public void testSelectCandidatesAboveThreshold() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = createSegments(dir, 12);
        CatalogSnapshot catalog = catalogOf(segments);

        MVCompactionService service = new MVCompactionService(null, null);
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 8);

        // 12 segments over threshold: pairwise policy selects exactly 2
        // (the adjacent pair with the smallest combined size, never K)
        assertEquals("pairwise compaction selects exactly 2", 2, candidates.size());

        // Verify they're sorted by generation ascending (oldest first)
        for (int i = 1; i < candidates.size(); i++) {
            assertTrue(
                "candidates must be generation-ordered",
                candidates.get(i).generation() >= candidates.get(i - 1).generation()
            );
        }

        // The last 2 generations (11, 12) must NOT appear
        for (MVCompactionService.CompactCandidate c : candidates) {
            assertTrue("gen 11 and 12 should be kept", c.generation() < 11);
        }
    }

    /**
     * When there are mixed segments (some with mv_state, some without),
     * only mv_state segments are considered.
     */
    public void testSelectCandidatesIgnoresNonMvStateSegments() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = new ArrayList<>();
        // 4 mv_state segments
        for (int i = 1; i <= 4; i++) {
            segments.add(createMvStateSegment(dir, i));
        }
        // 3 non-mv segments (e.g. parquet-only) — use a different format name
        for (int i = 5; i <= 7; i++) {
            WriterFileSet pqFiles = MonoFileWriterSet.of(dir, i, "parquet_" + i + ".parquet", 100);
            segments.add(Segment.builder(i).addSearchableFiles(new ParquetDataFormat(), pqFiles).build());
        }

        CatalogSnapshot catalog = catalogOf(segments);
        MVCompactionService service = new MVCompactionService(null, null);

        // threshold=2, 4 mv_state segments -> should compact 2 (keep last 2)
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 2);
        assertEquals("should select 2 oldest mv_state segments", 2, candidates.size());
        assertEquals(1L, candidates.get(0).generation());
        assertEquals(2L, candidates.get(1).generation());
    }

    /**
     * Edge case: exactly at threshold + 1 with only 3 mv_state segments.
     * Keep last 2 → only 1 candidate → not enough to compact (need >= 2).
     */
    public void testSelectCandidatesNeedsAtLeastTwoCandidates() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = createSegments(dir, 3);
        CatalogSnapshot catalog = catalogOf(segments);

        MVCompactionService service = new MVCompactionService(null, null);
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 2);
        assertEquals("need at least 2 candidates to compact", 0, candidates.size());
    }

    /**
     * With 10 segments and threshold=4, should compact 8 (keeping last 2).
     */
    public void testSelectCandidatesLargeSet() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = createSegments(dir, 10);
        CatalogSnapshot catalog = catalogOf(segments);

        MVCompactionService service = new MVCompactionService(null, null);
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 4);
        assertEquals("pairwise: exactly 2 per pass", 2, candidates.size());
    }

    /**
     * Files that don't exist on disk still get selected but with 0 size.
     */
    public void testSelectCandidatesHandlesMissingFiles() throws Exception {
        Path dir = createTempDir();
        List<Segment> segments = new ArrayList<>();
        for (int i = 1; i <= 6; i++) {
            String fileName = "_mv_poc_" + Long.toHexString(i) + ".mv.arrow";
            // Don't actually create the file on disk
            WriterFileSet fileSet = MonoFileWriterSet.of(dir, i, fileName, 100 * i);
            segments.add(Segment.builder(i).addSearchableFiles(MVStateDataFormat.INSTANCE, fileSet).build());
        }

        CatalogSnapshot catalog = catalogOf(segments);
        MVCompactionService service = new MVCompactionService(null, null);
        // threshold=3, 6 segments -> compact 4 (keep last 2)
        List<MVCompactionService.CompactCandidate> candidates = service.selectCandidates(catalog, 3);
        assertEquals("pairwise: exactly 2 per pass", 2, candidates.size());
        // Sizes should all be 0 since files don't exist
        for (MVCompactionService.CompactCandidate c : candidates) {
            assertEquals(0L, c.sizeBytes());
        }
    }

    /**
     * Verify MVBuildMetrics compaction counters.
     */
    public void testMetricsRecording() {
        MVBuildMetrics metrics = MVBuildMetrics.INSTANCE;
        metrics.reset();

        metrics.recordCompactionStarted();
        assertEquals(1, metrics.getCompactionsStarted());

        metrics.recordCompactionCompleted(5, 1000L, 200L, 800L, 50L);
        assertEquals(1, metrics.getCompactionsCompleted());

        metrics.recordCompactionFailed();
        assertEquals(1, metrics.getCompactionsFailed());

        metrics.recordCompactionSkipped();
        assertEquals(1, metrics.getCompactionsSkipped());

        Map<String, Long> snapshot = metrics.snapshot();
        assertEquals(Long.valueOf(1), snapshot.get("compactions_started"));
        assertEquals(Long.valueOf(1), snapshot.get("compactions_completed"));
        assertEquals(Long.valueOf(1), snapshot.get("compactions_failed"));
        assertEquals(Long.valueOf(1), snapshot.get("compactions_skipped"));
        assertEquals(Long.valueOf(5), snapshot.get("compaction_input_generations"));
        assertEquals(Long.valueOf(1000), snapshot.get("compaction_input_bytes"));
        assertEquals(Long.valueOf(200), snapshot.get("compaction_output_rows"));
        assertEquals(Long.valueOf(800), snapshot.get("compaction_output_bytes"));
        assertEquals(Long.valueOf(50), snapshot.get("compaction_duration_ms"));

        metrics.reset();
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private List<Segment> createSegments(Path dir, int count) throws Exception {
        List<Segment> segments = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            segments.add(createMvStateSegment(dir, i));
        }
        return segments;
    }

    private Segment createMvStateSegment(Path dir, long generation) throws Exception {
        String fileName = "_mv_poc_" + Long.toHexString(generation) + ".mv.arrow";
        Path file = dir.resolve(fileName);
        Files.writeString(file, "dummy-state-" + generation);
        WriterFileSet fileSet = MonoFileWriterSet.of(dir, generation, fileName, 100 * generation);
        return Segment.builder(generation).addSearchableFiles(MVStateDataFormat.INSTANCE, fileSet).build();
    }

    /** Minimal DataFormat stand-in for non-MV segments. */
    private static final class ParquetDataFormat extends org.opensearch.index.engine.dataformat.DataFormat {
        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public java.util.Set<org.opensearch.index.engine.dataformat.FieldTypeCapabilities> supportedFields() {
            return java.util.Set.of();
        }
    }
}
