/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.mv.MVStateDataFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;

/**
 * Utility for computing and registering pre-computed CRC32 checksums for
 * mv_state files. Called immediately after the native build or compaction
 * completes so that the upload path in {@code DataFormatAwareStoreDirectory}
 * and {@code RemoteSegmentStoreDirectory} can read the checksum in O(1)
 * instead of re-scanning the entire file.
 *
 * <p>This is the Java-side fix for defect #7: mv_state files never registered
 * a {@code FormatChecksumStrategy}, causing every publish and every restart
 * recovery to CRC32-scan the full catalog (~219 GB at gen-38, ~45 min).</p>
 */
final class MVStateChecksumUtil {

    private static final Logger logger = LogManager.getLogger(MVStateChecksumUtil.class);
    private static final int BUFFER_SIZE = 64 * 1024; // 64 KiB — same as PrecomputedChecksumStrategy

    private MVStateChecksumUtil() {}

    /**
     * Computes CRC32 of the completed mv_state file and registers it on the
     * shard's shared checksum strategy so that subsequent upload/recovery
     * checksum lookups return in O(1).
     *
     * @param completedFile   the finalized mv_state file on disk
     * @param fileName        the bare filename (e.g., "mv_gen_42.arrow")
     * @param writerGeneration the generation that produced this file
     * @param shard           the target shard (provides the shared strategy map)
     * @return the computed CRC32, or -1 if registration failed (non-fatal)
     */
    static long computeAndRegister(Path completedFile, String fileName, long writerGeneration, IndexShard shard) {
        FormatChecksumStrategy strategy = shard.getChecksumStrategies().get(MVStateDataFormat.NAME);
        if (strategy == null) {
            // Strategy not registered (should not happen after the plugin fix,
            // but fail-open — the generic fallback will still work, just O(n)).
            logger.warn("CHECKSUM_REGISTER_SKIP file=[{}] reason=no_strategy_for_mv_state", fileName);
            MVBuildMetrics.INSTANCE.recordChecksumMiss();
            return -1;
        }

        long checksum;
        try {
            checksum = computeFileCrc32(completedFile);
        } catch (IOException e) {
            logger.warn("CHECKSUM_COMPUTE_FAILED file=[{}] error=[{}]", fileName, e.getMessage(), e);
            MVBuildMetrics.INSTANCE.recordChecksumMiss();
            return -1;
        }

        // Register with the FileMetadata overload so the strategy owns key derivation.
        FileMetadata fm = new FileMetadata(MVStateDataFormat.NAME, fileName);
        strategy.registerChecksum(fm, checksum, writerGeneration);

        logger.debug(
            "CHECKSUM_REGISTERED file=[{}] checksum={} source=write generation={}",
            fileName,
            Long.toUnsignedString(checksum),
            writerGeneration
        );
        MVBuildMetrics.INSTANCE.recordChecksumRegistered();

        return checksum;
    }

    /**
     * Computes the full-file CRC32 of the given path using streaming reads.
     * For a ~500 MB generation at typical disk bandwidth (~240 MB/s), this
     * takes ~2 seconds — a one-time cost that eliminates repeated O(n) scans.
     */
    static long computeFileCrc32(Path file) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[BUFFER_SIZE];
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                crc32.update(buffer, 0, read);
            }
        }
        return crc32.getValue();
    }
}
