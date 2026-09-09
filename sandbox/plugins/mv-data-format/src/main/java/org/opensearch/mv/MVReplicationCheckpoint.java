/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * First-class MV replication checkpoint, modeled on SegRep's
 * {@link org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint}.
 *
 * <p>Carries the complete state of a source shard's parquet generation:
 * identity, term, sequence progress, and per-file metadata including
 * CRC32 for post-download verification.
 *
 * <h2>Ordering (isAheadOf / compareTo)</h2>
 * <ul>
 *   <li>null or EMPTY checkpoints are behind everything (fail-safe)</li>
 *   <li>primaryTerm dominates: higher term wins regardless of seqNo</li>
 *   <li>Equal term: higher maxSeqNo wins</li>
 *   <li>Equal term+seqNo: higher infosVersion wins</li>
 * </ul>
 *
 * <h2>Wire format</h2>
 * Compact: strings for source identity, ZLong for longs, VInt count + entries
 * for the file metadata map. Suitable for high-frequency inter-node pushes.
 *
 * <h2>Equality</h2>
 * Positional identity (sourceIndex, sourceShard, primaryTerm, maxSeqNo,
 * infosVersion) — NOT the metadata map, mirroring ReplicationCheckpoint.
 *
 * @opensearch.experimental
 */
public final class MVReplicationCheckpoint implements Writeable, Comparable<MVReplicationCheckpoint> {

    private final String sourceIndex;
    private final int sourceShard;
    private final long primaryTerm;
    private final long maxSeqNo;
    private final long infosVersion;
    private final Map<String, MVFileMetadata> fileMetadata;
    private final long createdTimeStampMillis;
    /**
     * Defect 13: seqNos in (targetWatermark, maxSeqNo] that consumed a sequence
     * number but produced no parquet row (failed index ops, deletes). Sorted
     * ascending. Delta-encoded on the wire (VLong deltas from previous value).
     * Empty array when no noops exist in the range — the common case.
     *
     * <p>The target's coverage check uses this to adjust the expected row count:
     * {@code expected = rangeSize - noopCount}.</p>
     */
    private final long[] noopSeqNos;

    /**
     * Full constructor.
     */
    public MVReplicationCheckpoint(
        String sourceIndex,
        int sourceShard,
        long primaryTerm,
        long maxSeqNo,
        long infosVersion,
        Map<String, MVFileMetadata> fileMetadata,
        long createdTimeStampMillis
    ) {
        this(sourceIndex, sourceShard, primaryTerm, maxSeqNo, infosVersion, fileMetadata, createdTimeStampMillis, EMPTY_NOOPS);
    }

    /**
     * Full constructor with noop seqNos.
     */
    public MVReplicationCheckpoint(
        String sourceIndex,
        int sourceShard,
        long primaryTerm,
        long maxSeqNo,
        long infosVersion,
        Map<String, MVFileMetadata> fileMetadata,
        long createdTimeStampMillis,
        long[] noopSeqNos
    ) {
        this.sourceIndex = sourceIndex;
        this.sourceShard = sourceShard;
        this.primaryTerm = primaryTerm;
        this.maxSeqNo = maxSeqNo;
        this.infosVersion = infosVersion;
        this.fileMetadata = fileMetadata == null ? Collections.emptyMap() : Map.copyOf(fileMetadata);
        this.createdTimeStampMillis = createdTimeStampMillis;
        this.noopSeqNos = noopSeqNos == null || noopSeqNos.length == 0 ? EMPTY_NOOPS : noopSeqNos.clone();
    }

    private static final long[] EMPTY_NOOPS = new long[0];

    /**
     * Creates an EMPTY sentinel checkpoint for the given source shard.
     * EMPTY is behind every non-EMPTY checkpoint (isAheadOf returns false).
     */
    public static MVReplicationCheckpoint empty(String sourceIndex, int sourceShard) {
        return new MVReplicationCheckpoint(
            sourceIndex, sourceShard, 0L, -1L, -1L, Collections.emptyMap(), System.currentTimeMillis(), EMPTY_NOOPS
        );
    }

    /** Returns true if this is an EMPTY sentinel (maxSeqNo == -1 and primaryTerm == 0). */
    public boolean isEmpty() {
        return primaryTerm == 0L && maxSeqNo == -1L;
    }

    // ── Wire format ──────────────────────────────────────────────────────

    public MVReplicationCheckpoint(StreamInput in) throws IOException {
        this.sourceIndex = in.readString();
        this.sourceShard = in.readVInt();
        this.primaryTerm = in.readZLong();
        this.maxSeqNo = in.readZLong();
        this.infosVersion = in.readZLong();
        int mapSize = in.readVInt();
        if (mapSize == 0) {
            this.fileMetadata = Collections.emptyMap();
        } else {
            Map<String, MVFileMetadata> map = new LinkedHashMap<>(mapSize);
            for (int i = 0; i < mapSize; i++) {
                String name = in.readString();
                MVFileMetadata meta = new MVFileMetadata(in);
                map.put(name, meta);
            }
            this.fileMetadata = Collections.unmodifiableMap(map);
        }
        this.createdTimeStampMillis = in.readZLong();
        // Defect 13: delta-encoded noop seqNos (usually empty)
        int noopCount = in.readVInt();
        if (noopCount == 0) {
            this.noopSeqNos = EMPTY_NOOPS;
        } else {
            this.noopSeqNos = new long[noopCount];
            long prev = 0;
            for (int i = 0; i < noopCount; i++) {
                prev += in.readVLong();
                this.noopSeqNos[i] = prev;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sourceIndex);
        out.writeVInt(sourceShard);
        out.writeZLong(primaryTerm);
        out.writeZLong(maxSeqNo);
        out.writeZLong(infosVersion);
        out.writeVInt(fileMetadata.size());
        for (Map.Entry<String, MVFileMetadata> entry : fileMetadata.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
        out.writeZLong(createdTimeStampMillis);
        // Defect 13: delta-encoded noop seqNos
        out.writeVInt(noopSeqNos.length);
        long prev = 0;
        for (long seqNo : noopSeqNos) {
            out.writeVLong(seqNo - prev);
            prev = seqNo;
        }
    }

    // ── Ordering ─────────────────────────────────────────────────────────

    /**
     * Returns true if this checkpoint is ahead of {@code other}.
     * Term dominates, then maxSeqNo, then infosVersion.
     * Returns true when other is null or EMPTY.
     *
     * <p>This is the primary ordering function used for mailbox coalescing
     * and consumer decisions — ensures failover correctness (a new-term
     * advert always supersedes an old-term one).
     */
    public boolean isAheadOf(@Nullable MVReplicationCheckpoint other) {
        if (other == null || other.isEmpty()) {
            return !this.isEmpty();
        }
        if (this.isEmpty()) {
            return false;
        }
        // Term dominates
        if (primaryTerm != other.primaryTerm) {
            return primaryTerm > other.primaryTerm;
        }
        // Same term: maxSeqNo dominates
        if (maxSeqNo != other.maxSeqNo) {
            return maxSeqNo > other.maxSeqNo;
        }
        // Same term + seqNo: infosVersion breaks tie
        return infosVersion > other.infosVersion;
    }

    @Override
    public int compareTo(MVReplicationCheckpoint other) {
        if (this.isAheadOf(other)) {
            return -1; // this is "better" (ahead) → sort first
        }
        if (other.isAheadOf(this)) {
            return 1;
        }
        return 0;
    }

    // ── Equality: positional identity, NOT the map ───────────────────────

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVReplicationCheckpoint that = (MVReplicationCheckpoint) o;
        return sourceShard == that.sourceShard
            && primaryTerm == that.primaryTerm
            && maxSeqNo == that.maxSeqNo
            && infosVersion == that.infosVersion
            && Objects.equals(sourceIndex, that.sourceIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceIndex, sourceShard, primaryTerm, maxSeqNo);
    }

    // ── Accessors ────────────────────────────────────────────────────────

    public String sourceIndex() {
        return sourceIndex;
    }

    public int sourceShard() {
        return sourceShard;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public long maxSeqNo() {
        return maxSeqNo;
    }

    public long infosVersion() {
        return infosVersion;
    }

    public Map<String, MVFileMetadata> fileMetadata() {
        return fileMetadata;
    }

    public long createdTimeStampMillis() {
        return createdTimeStampMillis;
    }

    /**
     * Sorted array of noop seqNos in this checkpoint's advertised range.
     * Empty when no noops exist — the common case.
     */
    public long[] noopSeqNos() {
        return noopSeqNos;
    }

    @Override
    public String toString() {
        return "MVReplicationCheckpoint{"
            + "source=" + sourceIndex + "[" + sourceShard + "]"
            + ", term=" + primaryTerm
            + ", maxSeqNo=" + maxSeqNo
            + ", infosVersion=" + infosVersion
            + ", files=" + fileMetadata.size()
            + ", noops=" + noopSeqNos.length
            + ", ts=" + createdTimeStampMillis
            + '}';
    }
}
