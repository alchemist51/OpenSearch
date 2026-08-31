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
        return appliedThrough >= currentSeqNo && totalRows == appliedThrough - currentSeqNo;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "MVWatermark[term=%d seqNo=%d gen=%d]", primaryTerm, seqNo, generation);
    }
}
