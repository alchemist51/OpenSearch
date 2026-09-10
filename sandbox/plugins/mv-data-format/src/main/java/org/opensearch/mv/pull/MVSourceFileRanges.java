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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Node-level cache of the {@code _seq_no} range of the source's parquet files, computed once per (immutable) file in the
 * background and advertised in the pull checkpoint so a builder skips files whose rows it has already applied.
 *
 * <p>Why: without ranges every merge output (2–3 GB, rows the builder consumed long ago from the flush files) is
 * advertised as UNKNOWN and downloaded. Measured 2026-09-10 on a 100M-document run: ~32 GB of merged files pulled per
 * view for 12.5 GB of new data — two thirds of the builder's network traffic was re-downloads. The scan reads only the
 * {@code _seq_no} column (≈1 s for a 3 GB file) and runs off the request thread; a file is UNKNOWN until its range is
 * known, which keeps today's behaviour as the fallback.</p>
 */
public final class MVSourceFileRanges {

    private static final Logger logger = LogManager.getLogger(MVSourceFileRanges.class);
    private static volatile MVSourceFileRanges instance;

    private final MVDataFusionReadEngine engine;
    private final Map<String, long[]> ranges = new ConcurrentHashMap<>();
    private final Set<String> pending = ConcurrentHashMap.newKeySet();

    private MVSourceFileRanges(Path workingPath) throws IOException {
        this.engine = new MVDataFusionReadEngine(workingPath);
    }

    /** Lazily created singleton under the node's temp directory (java.io.tmpdir). */
    public static MVSourceFileRanges get() {
        MVSourceFileRanges local = instance;
        if (local == null) {
            synchronized (MVSourceFileRanges.class) {
                local = instance;
                if (local == null) {
                    try {
                        Path work = Path.of(System.getProperty("java.io.tmpdir")).resolve("mv_pull_source_ranges");
                        Files.createDirectories(work);
                        local = new MVSourceFileRanges(work);
                    } catch (IOException e) {
                        logger.warn("mv_pull source file ranges unavailable: {}", e.getMessage());
                        return null;
                    }
                    instance = local;
                }
            }
        }
        return local;
    }

    /**
     * @return {min, max} of {@code _seq_no} for the file, or {@code null} while unknown (a computation is scheduled on
     *         {@code executor} the first time a file is asked for).
     */
    public long[] rangeOf(String key, Path file, Executor executor) {
        long[] known = ranges.get(key);
        if (known != null) {
            return known;
        }
        if (Files.exists(file) && pending.add(key)) {
            executor.execute(() -> {
                long start = System.nanoTime();
                try {
                    long[] r = engine.fileSeqNoRange(file, Integer.toHexString(key.hashCode()) + "-" + Thread.currentThread().getId());
                    ranges.put(key, r);
                    logger.info(
                        "mv_pull FILE_RANGE file=[{}] seq=({}, {}] ms={}",
                        key,
                        r[0] - 1,
                        r[1],
                        (System.nanoTime() - start) / 1_000_000L
                    );
                } catch (Exception e) {
                    logger.warn("mv_pull FILE_RANGE failed for [{}]: {}", key, e.getMessage());
                } finally {
                    pending.remove(key);
                }
            });
        }
        return null;
    }

    /** Drops entries for files that no longer exist locally (merged away); called opportunistically by the handler. */
    public void retainOnly(Set<String> liveKeys) {
        ranges.keySet().retainAll(liveKeys);
    }

    int size() {
        return ranges.size();
    }
}
