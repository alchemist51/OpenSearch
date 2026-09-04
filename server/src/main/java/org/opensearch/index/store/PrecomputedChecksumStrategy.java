/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

/**
 * Checksum strategy that uses pre-computed checksums when available,
 * falling back to full-file CRC32 scan.
 *
 * <p>Entries carry the writer generation that produced them; a newer gen overwrites an older
 * one, preventing stale registrations from clobbering fresh data after filename reuse.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class PrecomputedChecksumStrategy implements FormatChecksumStrategy {

    private static final Logger logger = LogManager.getLogger(PrecomputedChecksumStrategy.class);

    /** Sentinel generation for fallback-computed entries; any write-path register (gen &gt;= 1) overwrites. */
    private static final long FALLBACK_GENERATION = 0L;

    /** Cache entry: checksum + the generation that produced it. */
    private record CacheEntry(long checksum, long generation) {
    }

    private final Map<String, CacheEntry> checksumCache = new ConcurrentHashMap<>();

    @Override
    public long computeChecksum(Directory dir, String fileName) throws IOException {
        CacheEntry entry = checksumCache.get(fileName);
        if (entry != null) {
            logger.info(
                "CHECKSUM_LOOKUP_HIT instance={} key=[{}] gen={} cacheSize={}",
                System.identityHashCode(this),
                fileName,
                entry.generation(),
                checksumCache.size()
            );
            return entry.checksum();
        }
        // DIAGNOSTIC (2026-09-04 publish-slowness investigation): dump the requested
        // key vs present keys so key-form mismatches and instance churn are visible.
        logger.info(
            "CHECKSUM_LOOKUP_MISS instance={} requested=[{}] cacheSize={} presentKeys={}",
            System.identityHashCode(this),
            fileName,
            checksumCache.size(),
            checksumCache.keySet().stream().limit(8).toList()
        );
        long computed = computeFullFileCrc32(dir, fileName);
        checksumCache.putIfAbsent(fileName, new CacheEntry(computed, FALLBACK_GENERATION));
        return computed;
    }

    @Override
    public void registerChecksum(String fileName, long checksum, long writerGeneration) {
        if (fileName == null || checksum == 0) {
            return;
        }
        checksumCache.compute(fileName, (key, existing) -> {
            if (existing == null || writerGeneration >= existing.generation()) {
                return new CacheEntry(checksum, writerGeneration);
            }
            return existing;
        });
        logger.info(
            "CHECKSUM_REGISTER instance={} key=[{}] gen={} cacheSize={}",
            System.identityHashCode(this),
            fileName,
            writerGeneration,
            checksumCache.size()
        );
    }

    @Override
    public void clearChecksums() {
        logger.info("CHECKSUM_CLEAR instance={} sizeBefore={}", System.identityHashCode(this), checksumCache.size());
        checksumCache.clear();
    }

    /**
     * Removes a single checksum entry after it has been consumed (e.g., after successful upload).
     * Prevents unbounded cache growth over the shard's lifetime.
     *
     * @param fileName the file whose checksum should be evicted
     */
    public void evictChecksum(String fileName) {
        if (fileName != null) {
            checksumCache.remove(fileName);
        }
    }

    /**
     * Retains only checksums for files in the given set, evicting all others.
     * Called after successful upload with the current catalog snapshot's file set.
     *
     * @param activeFiles the set of files currently in the catalog snapshot
     */
    public void retainOnly(Collection<String> activeFiles) {
        int before = checksumCache.size();
        checksumCache.keySet().retainAll(activeFiles);
        logger.info(
            "CHECKSUM_RETAIN_ONLY instance={} sizeBefore={} sizeAfter={} activeSample={}",
            System.identityHashCode(this),
            before,
            checksumCache.size(),
            activeFiles.stream().limit(6).toList()
        );
    }

    private static long computeFullFileCrc32(Directory dir, String fileName) throws IOException {
        CRC32 crc32 = new CRC32();
        byte[] buffer = new byte[64 * 1024];
        try (IndexInput input = dir.openInput(fileName, IOContext.READONCE)) {
            long remaining = input.length();
            while (remaining > 0) {
                int toRead = (int) Math.min(buffer.length, remaining);
                input.readBytes(buffer, 0, toRead);
                crc32.update(buffer, 0, toRead);
                remaining -= toRead;
            }
        }
        return crc32.getValue();
    }
}
