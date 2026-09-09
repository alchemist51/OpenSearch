/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Per-source-shard pull watermark, committed atomically with the folded data
 * in the MV's Lucene commit userData. Key: {@code mv.wm.<sourceShardId>},
 * value: {@code <primaryTerm>:<foldedThroughSeqNo>:<sourceMetadataGeneration>}.
 *
 * <p>The watermark may under-claim (a crash between commits re-derives the
 * uncommitted tail; appends are keyed so overlap re-folds are idempotent)
 * but never over-claims: it is only advanced after the round's rows are
 * applied, and it becomes durable in the same commit as those rows.
 */
public record MVWatermark(long primaryTerm, long seqNo, long generation) {

    public static final String KEY_PREFIX = "mv.wm.";
    public static final MVWatermark EMPTY = new MVWatermark(-1L, -1L, -1L);

    public String encode() {
        return primaryTerm + ":" + seqNo + ":" + generation;
    }

    public static MVWatermark decode(String value) {
        String[] parts = Objects.requireNonNull(value, "watermark value").split(":");
        if (parts.length != 3) {
            throw new IllegalStateException("malformed mv watermark [" + value + "]");
        }
        return new MVWatermark(Long.parseLong(parts[0]), Long.parseLong(parts[1]), Long.parseLong(parts[2]));
    }

    public static String key(int sourceShardId) {
        return KEY_PREFIX + sourceShardId;
    }

    /** Reads all watermark entries from commit user data. Missing entries mean "never pulled". */
    public static MVWatermark fromCommitUserData(Map<String, String> userData, int sourceShardId) {
        String value = userData.get(key(sourceShardId));
        return value == null ? EMPTY : decode(value);
    }

    /**
     * Returns whether an append-only source range contains every sequence
     * number exactly once. The expected cardinality of
     * {@code (currentSeqNo, appliedThrough]} is their difference.
     */
    public static boolean hasCompleteCoverage(long currentSeqNo, long appliedThrough, long totalRows) {
        return hasCompleteCoverage(currentSeqNo, appliedThrough, totalRows, 0);
    }

    /**
     * Defect 13: noop-aware coverage check. Expected row count is reduced by the
     * number of noop seqNos in the range (seqNos that consumed a sequence number
     * but did not produce a parquet row — failed index ops, deletes).
     *
     * @param noopsInRange count of noop seqNos in (currentSeqNo, appliedThrough]
     */
    public static boolean hasCompleteCoverage(long currentSeqNo, long appliedThrough, long totalRows, int noopsInRange) {
        long rangeSize = appliedThrough - currentSeqNo;
        long expected = rangeSize - noopsInRange;
        return appliedThrough >= currentSeqNo && totalRows == expected;
    }

    /**
     * Defect 13: Counts noop seqNos in the half-open range (fromExclusive, toInclusive].
     * The input array must be sorted ascending (from the checkpoint). Uses binary
     * search for O(log n) performance.
     */
    public static int countNoopsInRange(long[] sortedNoops, long fromExclusive, long toInclusive) {
        if (sortedNoops == null || sortedNoops.length == 0) {
            return 0;
        }
        int lo = lowerBound(sortedNoops, fromExclusive + 1);
        int hi = upperBound(sortedNoops, toInclusive);
        return Math.max(0, hi - lo);
    }

    private static int lowerBound(long[] arr, long target) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] < target) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    private static int upperBound(long[] arr, long target) {
        int lo = 0, hi = arr.length;
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (arr[mid] <= target) lo = mid + 1;
            else hi = mid;
        }
        return lo;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "MVWatermark[term=%d seqNo=%d gen=%d]", primaryTerm, seqNo, generation);
    }
}
