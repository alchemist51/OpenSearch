/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * Metadata for a single MV state file within an {@link MVReplicationCheckpoint}.
 * Modeled on {@link org.opensearch.index.store.StoreFileMetadata} but carrying
 * MV-specific per-file sequence ranges and CRC32 for verification.
 *
 * <p>Immutable after construction. Wire format uses ZLong for compact
 * representation of typical values.
 */
public final class MVFileMetadata implements Writeable {

    /** Unknown / unavailable CRC32 sentinel. */
    public static final long CRC32_UNKNOWN = -1L;
    /** Unknown / unavailable size sentinel. */
    public static final long SIZE_UNKNOWN = -1L;
    /** Unknown / unavailable seq range sentinel. */
    public static final long SEQ_UNKNOWN = -1L;

    private final long sizeBytes;
    private final long minSeqNo;
    private final long maxSeqNo;
    private final long crc32;

    public MVFileMetadata(long sizeBytes, long minSeqNo, long maxSeqNo, long crc32) {
        this.sizeBytes = sizeBytes;
        this.minSeqNo = minSeqNo;
        this.maxSeqNo = maxSeqNo;
        this.crc32 = crc32;
    }

    public MVFileMetadata(StreamInput in) throws IOException {
        this.sizeBytes = in.readZLong();
        this.minSeqNo = in.readZLong();
        this.maxSeqNo = in.readZLong();
        this.crc32 = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeZLong(sizeBytes);
        out.writeZLong(minSeqNo);
        out.writeZLong(maxSeqNo);
        out.writeZLong(crc32);
    }

    public long sizeBytes() {
        return sizeBytes;
    }

    public long minSeqNo() {
        return minSeqNo;
    }

    public long maxSeqNo() {
        return maxSeqNo;
    }

    public long crc32() {
        return crc32;
    }

    /** Returns true if a CRC32 value is available for verification. */
    public boolean hasCrc32() {
        return crc32 != CRC32_UNKNOWN;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVFileMetadata that = (MVFileMetadata) o;
        return sizeBytes == that.sizeBytes
            && minSeqNo == that.minSeqNo
            && maxSeqNo == that.maxSeqNo
            && crc32 == that.crc32;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeBytes, minSeqNo, maxSeqNo, crc32);
    }

    @Override
    public String toString() {
        return "MVFileMetadata{size=" + sizeBytes + ", seq=[" + minSeqNo + "," + maxSeqNo + "], crc32=" + crc32 + "}";
    }
}
