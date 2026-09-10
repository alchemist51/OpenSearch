/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-local view of how far every pull-based derived view is behind its source shard, expressed in
 * <em>time</em>: the age of the oldest source document the view does not contain yet.
 *
 * <p>Two halves:
 * <ul>
 *   <li>a registry of the latest published watermark (highest source seqNo the view is queryable for)
 *       per derived target, keyed by the source shard it follows — written by the
 *       {@link DerivedShardPoller} after every publish, read by the source shard's engine;</li>
 *   <li>a {@link Tracker} the source engine feeds with (time, processed checkpoint) samples so the
 *       indexing time of any seqNo can be looked up and the view's staleness computed.</li>
 * </ul>
 *
 * <p>The registry is node-local: it covers the single-node deployment of the POC (source and target
 * primaries on the same node). In a multi-node cluster the watermark would have to travel with the
 * poller's checkpoint request or a dedicated transport action; the engine side would be unchanged.
 */
public final class DerivedViewLag {

    private DerivedViewLag() {}

    /** source index name + "/" + shard id → target index name → latest published watermark (seqNo, -1 = nothing yet). */
    private static final Map<String, Map<String, Long>> WATERMARKS = new ConcurrentHashMap<>();

    static String key(String sourceIndex, int sourceShard) {
        return sourceIndex + "/" + sourceShard;
    }

    /** Poller: the view {@code targetIndex} over {@code sourceIndex/shard} is now queryable up to {@code watermark}. */
    public static void record(String sourceIndex, int sourceShard, String targetIndex, long watermark) {
        WATERMARKS.computeIfAbsent(key(sourceIndex, sourceShard), k -> new ConcurrentHashMap<>()).merge(targetIndex, watermark, Math::max);
    }

    /** Poller closing (relocation, shutdown, view deleted): the view no longer constrains the source. */
    public static void remove(String sourceIndex, int sourceShard, String targetIndex) {
        Map<String, Long> views = WATERMARKS.get(key(sourceIndex, sourceShard));
        if (views != null) {
            views.remove(targetIndex);
            if (views.isEmpty()) {
                WATERMARKS.remove(key(sourceIndex, sourceShard), views);
            }
        }
    }

    /** Source engine: the lowest watermark among the views following this shard, or {@code Long.MAX_VALUE} when there is none. */
    public static long minWatermark(String sourceIndex, int sourceShard) {
        Map<String, Long> views = WATERMARKS.get(key(sourceIndex, sourceShard));
        if (views == null || views.isEmpty()) {
            return Long.MAX_VALUE;
        }
        long min = Long.MAX_VALUE;
        for (long w : views.values()) {
            min = Math.min(min, w);
        }
        return min;
    }

    /** Name of the slowest view (for log lines), or null. */
    public static String slowestView(String sourceIndex, int sourceShard) {
        Map<String, Long> views = WATERMARKS.get(key(sourceIndex, sourceShard));
        if (views == null || views.isEmpty()) {
            return null;
        }
        String name = null;
        long min = Long.MAX_VALUE;
        for (Map.Entry<String, Long> e : views.entrySet()) {
            if (e.getValue() < min) {
                min = e.getValue();
                name = e.getKey();
            }
        }
        return name;
    }

    public static void clearForTests() {
        WATERMARKS.clear();
    }

    /**
     * Ring of (time, highest processed seqNo) samples for one source shard. {@link #staleness(long, long, long)} answers
     * "how long ago did this shard index the oldest document the view is missing". Not thread-safe: the engine samples
     * under a single-sampler guard.
     */
    public static final class Tracker {
        /** A sample is kept only when at least this much time passed since the previous one (bounds memory and lookups). */
        private final long minSpacingNanos;
        private final int capacity;
        private final ArrayDeque<long[]> samples = new ArrayDeque<>(); // {timeNanos, seqNo}

        public Tracker(long minSpacingNanos, int capacity) {
            this.minSpacingNanos = minSpacingNanos;
            this.capacity = capacity;
        }

        /** Record that at {@code nowNanos} every seqNo up to {@code processedCheckpoint} had been indexed. */
        public void sample(long nowNanos, long processedCheckpoint) {
            long[] last = samples.peekLast();
            if (last != null) {
                if (processedCheckpoint <= last[1]) {
                    return; // nothing new was indexed: the earlier timestamp stays the truth for these seqNos
                }
                if (nowNanos - last[0] < minSpacingNanos) {
                    return;
                }
            }
            samples.addLast(new long[] { nowNanos, processedCheckpoint });
            while (samples.size() > capacity) {
                samples.pollFirst();
            }
        }

        /**
         * Seconds between now and the time this shard indexed seqNo {@code viewWatermark + 1} (the oldest document the
         * view lacks). 0 when the view is complete ({@code viewWatermark >= processedCheckpoint}) or nothing was sampled;
         * when the missing document is older than the ring remembers, the ring's full span is returned (a lower bound).
         */
        public double staleness(long nowNanos, long viewWatermark, long processedCheckpoint) {
            if (viewWatermark >= processedCheckpoint || samples.isEmpty()) {
                return 0.0;
            }
            final long missing = viewWatermark + 1;
            Iterator<long[]> it = samples.iterator();
            long[] first = samples.peekFirst();
            if (first[1] >= missing) {
                // already indexed before the oldest sample: at least the whole ring span old
                return Math.max(0.0, (nowNanos - first[0]) / 1e9);
            }
            while (it.hasNext()) {
                long[] s = it.next();
                if (s[1] >= missing) {
                    return Math.max(0.0, (nowNanos - s[0]) / 1e9);
                }
            }
            return 0.0; // the missing document is newer than the last sample (indexed within the last spacing interval)
        }

        public int size() {
            return samples.size();
        }
    }
}
