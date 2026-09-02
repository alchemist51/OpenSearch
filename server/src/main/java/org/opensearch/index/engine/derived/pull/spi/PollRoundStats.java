/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Per-round stage timing and counters for the derived pull pipeline.
 *
 * <p>Implementations are format-specific (MV, vector, etc.) but this interface
 * is generic — the server-level poller records cumulative totals and exposes
 * structured samples suitable for offline p50/p90/p95/p99 and peak-lag
 * correlation, without importing any format-specific classes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class PollRoundStats {

    private final Map<String, Long> stageNanos;
    private final Map<String, Long> counters;
    private final long totalNanos;

    private PollRoundStats(Map<String, Long> stageNanos, Map<String, Long> counters, long totalNanos) {
        this.stageNanos = Collections.unmodifiableMap(new LinkedHashMap<>(stageNanos));
        this.counters = Collections.unmodifiableMap(new LinkedHashMap<>(counters));
        this.totalNanos = totalNanos;
    }

    /** Returns stage durations in nanoseconds, keyed by stage name. */
    public Map<String, Long> stageNanos() {
        return stageNanos;
    }

    /** Returns arbitrary counters (rows processed, bytes downloaded, etc.). */
    public Map<String, Long> counters() {
        return counters;
    }

    /** Returns total round duration in nanoseconds. */
    public long totalNanos() {
        return totalNanos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("PollRoundStats{total=");
        sb.append(totalNanos / 1_000_000).append("ms");
        stageNanos.forEach((k, v) -> sb.append(", ").append(k).append('=').append(v / 1_000_000).append("ms"));
        counters.forEach((k, v) -> sb.append(", ").append(k).append('=').append(v));
        sb.append('}');
        return sb.toString();
    }

    /** Builder for assembling stage timings during a poll round. */
    public static final class Builder {
        private final Map<String, Long> stageNanos = new LinkedHashMap<>();
        private final Map<String, Long> counters = new LinkedHashMap<>();
        private long roundStartNanos = -1;

        /** Marks the start of the round. Call before any stage. */
        public Builder startRound() {
            this.roundStartNanos = System.nanoTime();
            return this;
        }

        /** Records a stage duration in nanoseconds. */
        public Builder stage(String name, long nanos) {
            stageNanos.put(name, nanos);
            return this;
        }

        /** Records an arbitrary counter. */
        public Builder counter(String name, long value) {
            counters.put(name, value);
            return this;
        }

        /** Builds the stats, computing total from round start if available. */
        public PollRoundStats build() {
            long total = roundStartNanos > 0 ? System.nanoTime() - roundStartNanos : 0;
            return new PollRoundStats(stageNanos, counters, total);
        }
    }
}
