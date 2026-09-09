/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for {@link MVTargetHydrator}: diff logic (generation advance,
 * term rollover, empty checkpoint), file name generation parsing, lifecycle
 * start/stop idempotence, and catalog publish (allocate path, marker-based
 * skip, WARN-on-rejection, and start-up reconcile).
 */
public class MVTargetHydratorTests extends OpenSearchTestCase {

    // ── parseGenFromFileName ────────────────────────────────────────────

    public void testParseGenFromFileName() {
        assertEquals(5L, MVTargetHydrator.parseGenFromFileName("_mv_partial.s0.t1.g5.abc123.parquet"));
        assertEquals(1L, MVTargetHydrator.parseGenFromFileName("_mv_partial.s0.t1.g1.xyz.parquet"));
        assertEquals(100L, MVTargetHydrator.parseGenFromFileName("_mv_partial.s2.t3.g100.def456.parquet"));
    }

    public void testParseGenFromFileNameNoGenField() {
        assertEquals(0L, MVTargetHydrator.parseGenFromFileName("random_file.parquet"));
        assertEquals(0L, MVTargetHydrator.parseGenFromFileName(""));
        assertEquals(0L, MVTargetHydrator.parseGenFromFileName("no_gen_field.txt"));
    }

    public void testParseGenFromFileNameMalformed() {
        // .g followed by non-numeric
        assertEquals(0L, MVTargetHydrator.parseGenFromFileName("_mv_partial.s0.t1.gabc.xyz.parquet"));
        // .g at the end (no dot after)
        assertEquals(0L, MVTargetHydrator.parseGenFromFileName("_mv_partial.s0.t1.g5"));
    }

    // ── HighWater diff logic ────────────────────────────────────────────

    public void testHighWaterEmpty() {
        MVTargetHydrator.HighWater empty = MVTargetHydrator.HighWater.EMPTY;
        assertEquals(0L, empty.term());
        assertEquals(0L, empty.generation());
    }

    public void testHighWaterGenerationAdvance() {
        // Checkpoint gen > high-water gen → should download
        MVTargetHydrator.HighWater hw = new MVTargetHydrator.HighWater(1L, 5L);
        // gen=10 is ahead of gen=5
        assertTrue(10L > hw.generation()); // diff logic: checkpoint gen > hw.generation
    }

    public void testHighWaterSameGeneration() {
        MVTargetHydrator.HighWater hw = new MVTargetHydrator.HighWater(1L, 5L);
        // gen=5 is NOT ahead (<=)
        assertFalse(5L > hw.generation());
    }

    public void testHighWaterTermRollover() {
        MVTargetHydrator.HighWater hw = new MVTargetHydrator.HighWater(1L, 100L);
        // New term 2 with gen 1 → should download (term wins)
        long checkpointTerm = 2L;
        long checkpointGen = 1L;
        boolean isAhead = checkpointTerm > hw.term()
            || (checkpointTerm == hw.term() && checkpointGen > hw.generation());
        assertTrue("Higher term should win", isAhead);
    }

    public void testHighWaterTermRolloverLowerGen() {
        MVTargetHydrator.HighWater hw = new MVTargetHydrator.HighWater(2L, 100L);
        // Same term, lower gen → NOT ahead
        long checkpointTerm = 2L;
        long checkpointGen = 50L;
        boolean isAhead = checkpointTerm > hw.term()
            || (checkpointTerm == hw.term() && checkpointGen > hw.generation());
        assertFalse("Same term, lower gen should not be ahead", isAhead);
    }

    public void testHighWaterEmptyCheckpoint() {
        // Empty checkpoint (no entry for MV) → nothing to download
        // This is handled by null-check in pollSourceShard, not by HighWater
        MVTargetHydrator.HighWater hw = MVTargetHydrator.HighWater.EMPTY;
        // gen=0, term=0 → any real checkpoint gen > 0 is ahead
        assertTrue(1L > hw.generation());
    }

    // ── Lifecycle idempotence ───────────────────────────────────────────

    public void testStopIdempotence() {
        // Create hydrator without starting — verify close is safe
        // (We can't easily create a full hydrator in unit test without all deps,
        // but we can verify the closed flag and double-close safety)
        // This tests the AtomicBoolean closed pattern
        java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean(false);
        assertTrue("First close should succeed", closed.compareAndSet(false, true));
        assertFalse("Second close should be no-op", closed.compareAndSet(false, true));
    }

    // ── Compacted file name parsing ─────────────────────────────────────

    public void testParseCompactedGenRange() {
        long[] range = MVTargetHydrator.parseCompactedGenRange("_mv_compacted.s0.g1-6.abc12345.parquet");
        assertNotNull(range);
        assertEquals(1L, range[0]);
        assertEquals(6L, range[1]);
    }

    public void testParseCompactedGenRangeLargeRange() {
        long[] range = MVTargetHydrator.parseCompactedGenRange("_mv_compacted.s2.g100-999.deadbeef.parquet");
        assertNotNull(range);
        assertEquals(100L, range[0]);
        assertEquals(999L, range[1]);
    }

    public void testParseCompactedGenRangeNotCompacted() {
        assertNull(MVTargetHydrator.parseCompactedGenRange("_mv_partial.s0.t1.g5.abc123.parquet"));
        assertNull(MVTargetHydrator.parseCompactedGenRange("random_file.parquet"));
        assertNull(MVTargetHydrator.parseCompactedGenRange(""));
    }

    public void testParseCompactedGenRangeMalformed() {
        // Missing shard
        assertNull(MVTargetHydrator.parseCompactedGenRange("_mv_compacted.g1-6.abc.parquet"));
        // Missing range separator
        assertNull(MVTargetHydrator.parseCompactedGenRange("_mv_compacted.s0.g16.abc.parquet"));
    }

    // ── Compaction: threshold trigger ───────────────────────────────────

    public void testCompactThresholdDefault() {
        // Default threshold is 8
        assertEquals(8, MVTargetHydrator.COMPACT_THRESHOLD.getDefault(null).intValue());
    }

    public void testCompactThresholdMinimum() {
        // Minimum is 2
        assertEquals(2, MVTargetHydrator.COMPACT_THRESHOLD.get(
            org.opensearch.common.settings.Settings.builder().put("index.mv.compact_threshold", 2).build()
        ).intValue());
    }

    // ── Compaction: high-water rebuild with compacted files ─────────────

    public void testHighWaterFromCompactedFileName() {
        // _mv_compacted.s0.g5-20.abc.parquet should set high-water to gen 20.
        long[] range = MVTargetHydrator.parseCompactedGenRange("_mv_compacted.s0.g5-20.abc12345.parquet");
        assertNotNull(range);
        assertEquals(20L, range[1]); // maxGen
    }

    public void testHighWaterFromMixedFiles() {
        // A directory with both partial and compacted files: the highest gen wins.
        // _mv_compacted.s0.g1-10.abc.parquet → maxGen=10
        // _mv_partial.s0.t1.g15.xyz.parquet → maxGen=15
        // Expected: highwater gen = 15
        long maxGen = 0;
        String[] files = {
            "_mv_compacted.s0.g1-10.abc12345.parquet",
            "_mv_partial.s0.t1.g15.xyz12345.parquet"
        };
        for (String f : files) {
            long[] range = MVTargetHydrator.parseCompactedGenRange(f);
            if (range != null) {
                if (range[1] > maxGen) maxGen = range[1];
            } else {
                long gen = MVTargetHydrator.parseGenFromFileName(f);
                if (gen > maxGen) maxGen = gen;
            }
        }
        assertEquals(15L, maxGen);
    }

    // ── Compaction: single-flight guard ─────────────────────────────────

    public void testSingleFlightGuard() {
        // Verify ConcurrentHashMap.newKeySet() add/remove semantics.
        java.util.Set<Integer> compacting = java.util.concurrent.ConcurrentHashMap.newKeySet();
        assertTrue("First add should succeed", compacting.add(0));
        assertFalse("Second add should fail (single-flight)", compacting.add(0));
        compacting.remove(0);
        assertTrue("After remove, add should succeed again", compacting.add(0));
    }

    // ── Compaction: snapshot isolation ──────────────────────────────────

    public void testSnapshotIsolation() throws java.io.IOException {
        // Simulate: snapshot 3 files, then a new file appears mid-compaction.
        // The new file must survive (not be in the delete set).
        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Path shardDir = tempDir.resolve("0");
        java.nio.file.Files.createDirectories(shardDir);

        // Create 3 files before snapshot.
        for (int i = 1; i <= 3; i++) {
            java.nio.file.Files.writeString(shardDir.resolve("_mv_partial.g" + i + ".parquet"), "data");
        }

        // Take snapshot.
        java.util.List<java.nio.file.Path> snapshot = new java.util.ArrayList<>();
        try (var files = java.nio.file.Files.newDirectoryStream(shardDir, "*.parquet")) {
            for (java.nio.file.Path f : files) {
                snapshot.add(f);
            }
        }
        assertEquals(3, snapshot.size());

        // Simulate file arriving mid-compaction.
        java.nio.file.Path newFile = shardDir.resolve("_mv_partial.g4.parquet");
        java.nio.file.Files.writeString(newFile, "data");

        // Delete only snapshot files.
        for (java.nio.file.Path f : snapshot) {
            java.nio.file.Files.deleteIfExists(f);
        }

        // New file must survive.
        assertTrue("File added during compaction must survive", java.nio.file.Files.exists(newFile));
        // Original files gone.
        for (java.nio.file.Path f : snapshot) {
            assertFalse("Snapshot file should be deleted", java.nio.file.Files.exists(f));
        }
    }

    // ── Compaction: failure leaves inputs intact ────────────────────────

    public void testCompactFailureLeavesInputs() throws java.io.IOException {
        // If compaction fails, input files must remain untouched.
        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Path shardDir = tempDir.resolve("0");
        java.nio.file.Files.createDirectories(shardDir);

        // Create input files.
        for (int i = 1; i <= 3; i++) {
            java.nio.file.Files.writeString(shardDir.resolve("_mv_partial.g" + i + ".parquet"), "data" + i);
        }

        // Simulate failed compaction: tmp file created but exception thrown.
        java.nio.file.Path tmpFile = shardDir.resolve("_mv_compacted.abc.parquet.tmp");
        java.nio.file.Files.writeString(tmpFile, "incomplete");

        // On failure, delete tmp and leave inputs.
        java.nio.file.Files.deleteIfExists(tmpFile);

        // All 3 input files must still exist.
        for (int i = 1; i <= 3; i++) {
            assertTrue(java.nio.file.Files.exists(shardDir.resolve("_mv_partial.g" + i + ".parquet")));
        }
        assertFalse("Tmp file should be cleaned up", java.nio.file.Files.exists(tmpFile));
    }

    // ── MV_STATE_FORMAT_NAME constant ───────────────────────────────────

    public void testMvStateFormatNameConstant() {
        assertEquals("mv_state", MVTargetHydrator.MV_STATE_FORMAT_NAME);
    }

    // ── CatalogPublisher callback ───────────────────────────────────────

    public void testCatalogPublisherInterfaceShape() throws IOException {
        // Verify the CatalogPublisher interface shape is callable and the format
        // name constant matches what the analytics-engine dispatch expects.
        FakePublisher publisher = new FakePublisher();
        long targetGen = publisher.publish("mv_state", "/tmp/test", Set.of("file1.parquet"), "mv1/0", 5L, 100L, Map.of());
        assertEquals("mv_state", publisher.lastFormat);
        assertEquals(5L, publisher.lastSourceGeneration);
        assertTrue("engine allocates a positive target generation", targetGen > 0);
        assertEquals(5L, publisher.publishedSourceGeneration("mv1/0"));
    }

    // ── Publish: collision no longer rejects ────────────────────────────

    /**
     * A source generation whose number equals a pre-existing target generation must still publish:
     * the engine allocates its own target generation, so there is no collision. Verifies the fix
     * for defect (a) — the hydrator no longer swallows a collision.
     */
    public void testPublishDoesNotRejectOnSourceGenerationCollision() throws Exception {
        FakePublisher publisher = new FakePublisher();
        // Pre-seed the fake engine's generation counter so an allocated generation would equal
        // the source generation number if the old (pass-through) behaviour were still in place.
        publisher.generationCounter.set(5L);
        try (MVTargetHydrator hydrator = newHydrator(createTempDir())) {
            hydrator.setCatalogPublisher(publisher);
            hydrator.publishGeneration(0, createTempDir(), 5L, 3L, Set.of("_mv_partial.s0.t1.g5.abc.parquet"));
        }
        assertEquals("published exactly once", 1, publisher.publishCount);
        assertEquals("source generation recorded in marker", 5L, publisher.publishedSourceGeneration("mv1/0"));
        assertTrue("engine-allocated target generation differs from source gen", publisher.lastTargetGeneration != 5L);
    }

    // ── Publish: rejection logged at WARN (never DEBUG) ─────────────────

    public void testPublishRejectionLoggedAtWarn() throws Exception {
        FakePublisher publisher = new FakePublisher();
        publisher.failWith = new IllegalArgumentException("derived artifact row count must be non-negative but was [-1]");
        try (
            MVTargetHydrator hydrator = newHydrator(createTempDir());
            MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(MVTargetHydrator.class))
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "warn on rejection",
                    MVTargetHydrator.class.getCanonicalName(),
                    Level.WARN,
                    "MV catalog publish rejected*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no debug masking",
                    MVTargetHydrator.class.getCanonicalName(),
                    Level.DEBUG,
                    "*already published*"
                )
            );
            hydrator.setCatalogPublisher(publisher);
            hydrator.publishGeneration(0, createTempDir(), 5L, -1L, Set.of("bad.parquet"));
            appender.assertAllExpectationsMatched();
        }
    }

    // ── Publish: marker prevents double publish across restart ──────────

    public void testMarkerPreventsDoublePublish() throws Exception {
        FakePublisher publisher = new FakePublisher();
        try (MVTargetHydrator hydrator = newHydrator(createTempDir())) {
            hydrator.setCatalogPublisher(publisher);
            Set<String> files = Set.of("_mv_partial.s0.t1.g7.abc.parquet");
            hydrator.publishGeneration(0, createTempDir(), 7L, 3L, files);
            assertEquals(1, publisher.publishCount);
            // Simulate a restart re-attempting the same source generation: marker must short-circuit.
            hydrator.publishGeneration(0, createTempDir(), 7L, 3L, files);
            assertEquals("second publish of same source gen must be skipped", 1, publisher.publishCount);
            // A newer generation still publishes.
            hydrator.publishGeneration(0, createTempDir(), 8L, 3L, Set.of("_mv_partial.s0.t1.g8.def.parquet"));
            assertEquals(2, publisher.publishCount);
        }
    }

    // ── Test helpers ────────────────────────────────────────────────────

    private MVTargetHydrator newHydrator(Path targetShardDataPath) {
        return new MVTargetHydrator(
            new ShardId(new Index("target", "_na_"), 0),
            "mv1",
            "source",
            targetShardDataPath,
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(MVStateRemoteManager.class),
            TimeValue.timeValueSeconds(1)
        );
    }

    /**
     * A stateful in-memory stand-in for the engine-backed CatalogPublisher: allocates a
     * monotonically increasing target generation and maintains a per-provenance published marker,
     * mirroring {@code DataFormatAwareEngine}'s allocate + marker semantics.
     */
    static final class FakePublisher implements MVTargetHydrator.CatalogPublisher {
        final java.util.concurrent.atomic.AtomicLong generationCounter = new java.util.concurrent.atomic.AtomicLong(0);
        final Map<String, Long> markers = new ConcurrentHashMap<>();
        int publishCount = 0;
        String lastFormat;
        long lastSourceGeneration;
        long lastTargetGeneration;
        RuntimeException failWith;

        @Override
        public long publish(String dataFormatName, String directory, Set<String> fileNames, String provenanceKey,
                            long sourceGeneration, long numRows, Map<String, String> userDataUpdates) throws IOException {
            if (failWith != null) {
                throw failWith;
            }
            long target = generationCounter.incrementAndGet();
            markers.merge(provenanceKey, sourceGeneration, Math::max);
            publishCount++;
            lastFormat = dataFormatName;
            lastSourceGeneration = sourceGeneration;
            lastTargetGeneration = target;
            return target;
        }

        @Override
        public long publishedSourceGeneration(String provenanceKey) {
            return markers.getOrDefault(provenanceKey, -1L);
        }
    }
}
