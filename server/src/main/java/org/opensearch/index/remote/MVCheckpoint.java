/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Checkpoint of MV partial state on a source shard, derived ONLY from
 * remote-committed manifests (defect-#30 invariant: never advertise
 * local in-flight state).
 *
 * <p>Updated atomically after {@link MVStateRemoteManager#uploadGeneration}
 * succeeds (manifest committed). On restart/promotion, rebuilt from
 * {@link MVStateRemoteManager#listManifests}.</p>
 *
 * @opensearch.internal
 */
public class MVCheckpoint implements Writeable {

    private final ShardId shardId;
    private final long primaryTerm;
    private final long maxSeqNo;
    private final long defMetadataVersion;
    private final Map<String, MVPartialEntry> entries;

    public MVCheckpoint(
        ShardId shardId,
        long primaryTerm,
        long maxSeqNo,
        long defMetadataVersion,
        Map<String, MVPartialEntry> entries
    ) {
        this.shardId = Objects.requireNonNull(shardId);
        this.primaryTerm = primaryTerm;
        this.maxSeqNo = maxSeqNo;
        this.defMetadataVersion = defMetadataVersion;
        this.entries = Collections.unmodifiableMap(Objects.requireNonNull(entries));
    }

    public MVCheckpoint(StreamInput in) throws IOException {
        this.shardId = new ShardId(in);
        this.primaryTerm = in.readVLong();
        this.maxSeqNo = in.readLong();
        this.defMetadataVersion = in.readVLong();
        int size = in.readVInt();
        Map<String, MVPartialEntry> map = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String mvId = in.readString();
            MVPartialEntry entry = new MVPartialEntry(in);
            map.put(mvId, entry);
        }
        this.entries = Collections.unmodifiableMap(map);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeVLong(primaryTerm);
        out.writeLong(maxSeqNo);
        out.writeVLong(defMetadataVersion);
        out.writeVInt(entries.size());
        for (Map.Entry<String, MVPartialEntry> e : entries.entrySet()) {
            out.writeString(e.getKey());
            e.getValue().writeTo(out);
        }
    }

    public ShardId shardId() {
        return shardId;
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public long maxSeqNo() {
        return maxSeqNo;
    }

    public long defMetadataVersion() {
        return defMetadataVersion;
    }

    public Map<String, MVPartialEntry> entries() {
        return entries;
    }

    /**
     * Returns true if this checkpoint is ahead of {@code other} for the given mvId.
     * Comparison: (primaryTerm, generation) lexicographic. Unknown mvId → false.
     */
    public boolean isAheadOf(MVCheckpoint other, String mvId) {
        MVPartialEntry mine = entries.get(mvId);
        if (mine == null) {
            return false;
        }
        MVPartialEntry theirs = other == null ? null : other.entries.get(mvId);
        if (theirs == null) {
            return true; // we have it, they don't
        }
        if (primaryTerm != other.primaryTerm) {
            return primaryTerm > other.primaryTerm;
        }
        return mine.generation() > theirs.generation();
    }

    /**
     * Rebuild checkpoint from remote manifests (restart/promotion path).
     */
    public static MVCheckpoint rebuildFromManifests(
        ShardId shardId,
        long primaryTerm,
        long defMetadataVersion,
        MVStateRemoteManager remoteManager,
        Map<String, String> mvDefinitions
    ) throws IOException {
        Map<String, MVPartialEntry> entries = new HashMap<>();
        long maxSeqNo = -1;

        for (String mvId : mvDefinitions.keySet()) {
            List<String> manifests = remoteManager.listManifests(String.valueOf(shardId.id()), mvId);
            if (manifests.isEmpty()) {
                continue;
            }
            // First = newest (inverted naming)
            String latestManifestName = manifests.get(0);
            MVStateManifest manifest = remoteManager.readManifest(
                String.valueOf(shardId.id()), mvId, latestManifestName
            );

            List<MVStateManifest.FileEntry> manifestFiles = manifest.files();
            long rowCount = 0;
            long sizeBytes = 0;
            List<FileInfo> files = manifestFiles.stream()
                .map(f -> new FileInfo(f.name(), f.length(), f.checksum()))
                .toList();
            for (MVStateManifest.FileEntry f : manifestFiles) {
                sizeBytes += f.length();
            }

            entries.put(mvId, new MVPartialEntry(
                manifest.generation(),
                manifest.maxSeqNo(),
                files,
                rowCount,
                sizeBytes
            ));

            if (manifest.maxSeqNo() > maxSeqNo) {
                maxSeqNo = manifest.maxSeqNo();
            }
        }

        return new MVCheckpoint(shardId, primaryTerm, maxSeqNo, defMetadataVersion, entries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVCheckpoint that = (MVCheckpoint) o;
        return primaryTerm == that.primaryTerm
            && maxSeqNo == that.maxSeqNo
            && defMetadataVersion == that.defMetadataVersion
            && shardId.equals(that.shardId)
            && entries.equals(that.entries);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, primaryTerm, maxSeqNo, defMetadataVersion, entries);
    }

    @Override
    public String toString() {
        return "MVCheckpoint{shard=" + shardId + ", term=" + primaryTerm
            + ", maxSeqNo=" + maxSeqNo + ", mvs=" + entries.size() + "}";
    }

    // ── Inner types ──────────────────────────────────────────────────────

    /**
     * Per-MV entry in the checkpoint.
     */
    public static class MVPartialEntry implements Writeable {
        private final long generation;
        private final long maxSeqNo;
        private final List<FileInfo> files;
        private final long rowCount;
        private final long sizeBytes;

        public MVPartialEntry(long generation, long maxSeqNo, List<FileInfo> files, long rowCount, long sizeBytes) {
            this.generation = generation;
            this.maxSeqNo = maxSeqNo;
            this.files = Collections.unmodifiableList(Objects.requireNonNull(files));
            this.rowCount = rowCount;
            this.sizeBytes = sizeBytes;
        }

        public MVPartialEntry(StreamInput in) throws IOException {
            this.generation = in.readVLong();
            this.maxSeqNo = in.readLong();
            this.files = in.readList(FileInfo::new);
            this.rowCount = in.readVLong();
            this.sizeBytes = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(generation);
            out.writeLong(maxSeqNo);
            out.writeCollection(files);
            out.writeVLong(rowCount);
            out.writeVLong(sizeBytes);
        }

        public long generation() { return generation; }
        public long maxSeqNo() { return maxSeqNo; }
        public List<FileInfo> files() { return files; }
        public long rowCount() { return rowCount; }
        public long sizeBytes() { return sizeBytes; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MVPartialEntry that = (MVPartialEntry) o;
            return generation == that.generation && maxSeqNo == that.maxSeqNo
                && rowCount == that.rowCount && sizeBytes == that.sizeBytes
                && files.equals(that.files);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generation, maxSeqNo, files, rowCount, sizeBytes);
        }

        @Override
        public String toString() {
            return "MVPartialEntry{gen=" + generation + ", seqNo=" + maxSeqNo
                + ", files=" + files.size() + ", rows=" + rowCount + "}";
        }
    }

    /**
     * File metadata within a checkpoint entry.
     */
    public static class FileInfo implements Writeable {
        private final String name;
        private final long length;
        private final String checksum;

        public FileInfo(String name, long length, String checksum) {
            this.name = Objects.requireNonNull(name);
            this.length = length;
            this.checksum = Objects.requireNonNull(checksum);
        }

        public FileInfo(StreamInput in) throws IOException {
            this.name = in.readString();
            this.length = in.readVLong();
            this.checksum = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(length);
            out.writeString(checksum);
        }

        public String name() { return name; }
        public long length() { return length; }
        public String checksum() { return checksum; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileInfo fi = (FileInfo) o;
            return length == fi.length && name.equals(fi.name) && checksum.equals(fi.checksum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length, checksum);
        }

        @Override
        public String toString() {
            return "FileInfo{'" + name + "', " + length + "B}";
        }
    }
}
