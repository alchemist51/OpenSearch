/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

/**
 * Unit tests for the mv_state O(1) checksum strategy:
 * <ul>
 *   <li>{@link MVStateChecksumUtil#computeFileCrc32(Path)} correctness</li>
 *   <li>Registered-hit: pre-computed checksum returned without file scan</li>
 *   <li>Miss-compute-once: first call scans, second call is O(1)</li>
 *   <li>Integration: write-path registration visible to the strategy</li>
 * </ul>
 */
public class MVStateChecksumUtilTests extends OpenSearchTestCase {

    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir("mv-checksum-test");
    }

    /**
     * Verify that computeFileCrc32 returns the correct CRC32 for known content.
     */
    public void testComputeFileCrc32_MatchesJavaCrc32() throws IOException {
        byte[] payload = "hello-mv-state-world-12345".getBytes(StandardCharsets.UTF_8);
        Path file = tempDir.resolve("test.arrow");
        Files.write(file, payload);

        CRC32 expected = new CRC32();
        expected.update(payload);

        long actual = MVStateChecksumUtil.computeFileCrc32(file);
        assertEquals(expected.getValue(), actual);
    }

    /**
     * Verify that computeFileCrc32 works for empty files.
     */
    public void testComputeFileCrc32_EmptyFile() throws IOException {
        Path file = tempDir.resolve("empty.arrow");
        Files.write(file, new byte[0]);

        CRC32 expected = new CRC32();
        // CRC32 of empty input = 0
        assertEquals(0L, MVStateChecksumUtil.computeFileCrc32(file));
        assertEquals(expected.getValue(), MVStateChecksumUtil.computeFileCrc32(file));
    }

    /**
     * Verify that computeFileCrc32 works for a large file (> buffer size).
     */
    public void testComputeFileCrc32_LargeFile() throws IOException {
        // Create a file larger than the 64KB buffer
        byte[] payload = new byte[128 * 1024 + 37]; // 128KB + 37 bytes (not aligned)
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) (i % 256);
        }
        Path file = tempDir.resolve("large.arrow");
        Files.write(file, payload);

        CRC32 expected = new CRC32();
        expected.update(payload);

        assertEquals(expected.getValue(), MVStateChecksumUtil.computeFileCrc32(file));
    }

    /**
     * Strategy returns the registered checksum in O(1) without scanning.
     */
    public void testRegisteredChecksum_ReturnedWithoutScanning() throws IOException {
        PrecomputedChecksumStrategy strategy = new PrecomputedChecksumStrategy();

        // Register a checksum for an mv_state file
        FileMetadata fm = new FileMetadata(MVStateDataFormat.NAME, "mv_gen_42.arrow");
        long registeredChecksum = 0xDEADBEEFL;
        strategy.registerChecksum(fm, registeredChecksum, 42L);

        // The strategy should return the registered value without needing a Directory
        // (it won't even try to open the file because the cache has the entry).
        // We can't call computeChecksum without a real Directory, but we can
        // register and then verify via a second register + compute pattern.
        // Actually, we CAN test this with a ByteBuffersDirectory:
        org.apache.lucene.store.ByteBuffersDirectory dir = new org.apache.lucene.store.ByteBuffersDirectory();
        // Write a file with DIFFERENT content to prove the strategy returns
        // the registered value, not the file scan.
        String key = fm.serialize(); // "mv_state/mv_gen_42.arrow"
        try (var out = dir.createOutput(key, org.apache.lucene.store.IOContext.DEFAULT)) {
            out.writeBytes("different-content".getBytes(StandardCharsets.UTF_8), 17);
        }

        long result = strategy.computeChecksum(dir, key);
        assertEquals("must return registered checksum, not file scan", registeredChecksum, result);
        dir.close();
    }

    /**
     * On cache miss, strategy computes CRC32 once, then returns it on
     * subsequent calls without re-scanning (O(1) after first access).
     */
    public void testMissComputeOnce_SecondCallIsO1() throws IOException {
        PrecomputedChecksumStrategy strategy = new PrecomputedChecksumStrategy();

        byte[] payload = "mv-state-gen-7-data".getBytes(StandardCharsets.UTF_8);
        CRC32 expected = new CRC32();
        expected.update(payload);
        long expectedChecksum = expected.getValue();

        // Use a ByteBuffersDirectory so the strategy can actually read the file
        org.apache.lucene.store.ByteBuffersDirectory dir = new org.apache.lucene.store.ByteBuffersDirectory();
        String fileName = "mv_state/mv_gen_7.arrow";
        try (var out = dir.createOutput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
            out.writeBytes(payload, payload.length);
        }

        // First call: cache miss, scans the file
        long first = strategy.computeChecksum(dir, fileName);
        assertEquals(expectedChecksum, first);

        // Delete the file from the directory — if the strategy re-scans, it will throw
        dir.deleteFile(fileName);

        // Second call: must return cached value (O(1), no file access)
        long second = strategy.computeChecksum(dir, fileName);
        assertEquals(expectedChecksum, second);
        dir.close();
    }

    /**
     * Write-path registration overwrites a fallback-computed entry.
     */
    public void testWriteRegistration_OverwritesFallback() throws IOException {
        PrecomputedChecksumStrategy strategy = new PrecomputedChecksumStrategy();

        byte[] payload = "some-content".getBytes(StandardCharsets.UTF_8);
        org.apache.lucene.store.ByteBuffersDirectory dir = new org.apache.lucene.store.ByteBuffersDirectory();
        String key = "mv_state/mv_gen_1.arrow";
        try (var out = dir.createOutput(key, org.apache.lucene.store.IOContext.DEFAULT)) {
            out.writeBytes(payload, payload.length);
        }

        // Prime the cache with the fallback (generation 0)
        long scanned = strategy.computeChecksum(dir, key);

        // Write path registers a different value with a real generation
        FileMetadata fm = new FileMetadata(MVStateDataFormat.NAME, "mv_gen_1.arrow");
        strategy.registerChecksum(fm, 999L, 5L);

        // Must return the write-path value, not the scan
        assertEquals(999L, strategy.computeChecksum(dir, key));
        assertNotEquals(scanned, 999L);
        dir.close();
    }

    /**
     * Verify metrics counters are incremented.
     */
    public void testMetricsCounters() {
        MVBuildMetrics metrics = new MVBuildMetrics();
        assertEquals(0, metrics.getChecksumRegistered());
        assertEquals(0, metrics.getChecksumMisses());

        metrics.recordChecksumRegistered();
        metrics.recordChecksumRegistered();
        metrics.recordChecksumMiss();

        assertEquals(2, metrics.getChecksumRegistered());
        assertEquals(1, metrics.getChecksumMisses());

        // Verify snapshot includes the new counters
        var snapshot = metrics.snapshot();
        assertEquals(Long.valueOf(2), snapshot.get("checksum_registered"));
        assertEquals(Long.valueOf(1), snapshot.get("checksum_misses"));

        metrics.reset();
        assertEquals(0, metrics.getChecksumRegistered());
        assertEquals(0, metrics.getChecksumMisses());
    }
}
