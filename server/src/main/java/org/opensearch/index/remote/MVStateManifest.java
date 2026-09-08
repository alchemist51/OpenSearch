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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Manifest for one generation of MV partial state in remote store.
 *
 * <p>Written last (after all data files) to enforce the invisibility rule:
 * no manifest ⟹ generation invisible. Readers list manifests to discover
 * committed generations.</p>
 *
 * <p>File name pattern:
 * {@code mv_manifest__<invertedTerm>__<invertedGen>__<uuid>}</p>
 *
 * @opensearch.internal
 */
public class MVStateManifest implements Writeable {

    /** Codec name for versioned serialization. */
    public static final String CODEC_NAME = "mv_state_manifest";
    /** Current codec version. */
    public static final int CODEC_VERSION = 1;

    private final long primaryTerm;
    private final long generation;
    private final long maxSeqNo;
    private final long defMetadataVersion;
    private final String definitionHash;
    private final List<FileEntry> files;

    public MVStateManifest(
        long primaryTerm,
        long generation,
        long maxSeqNo,
        long defMetadataVersion,
        String definitionHash,
        List<FileEntry> files
    ) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
        this.maxSeqNo = maxSeqNo;
        this.defMetadataVersion = defMetadataVersion;
        this.definitionHash = Objects.requireNonNull(definitionHash);
        this.files = Collections.unmodifiableList(Objects.requireNonNull(files));
    }

    public MVStateManifest(StreamInput in) throws IOException {
        this.primaryTerm = in.readVLong();
        this.generation = in.readVLong();
        this.maxSeqNo = in.readLong();
        this.defMetadataVersion = in.readVLong();
        this.definitionHash = in.readString();
        this.files = in.readList(FileEntry::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
        out.writeLong(maxSeqNo);
        out.writeVLong(defMetadataVersion);
        out.writeString(definitionHash);
        out.writeCollection(files);
    }

    public long primaryTerm() {
        return primaryTerm;
    }

    public long generation() {
        return generation;
    }

    public long maxSeqNo() {
        return maxSeqNo;
    }

    public long defMetadataVersion() {
        return defMetadataVersion;
    }

    public String definitionHash() {
        return definitionHash;
    }

    public List<FileEntry> files() {
        return files;
    }

    /**
     * Build the manifest file name. Uses inverted (Long.MAX - value) encoding
     * so that lexicographic listing returns newest-first.
     */
    public static String buildFileName(long primaryTerm, long generation, String uuid) {
        return String.format(
            "mv_manifest__%020d__%020d__%s",
            Long.MAX_VALUE - primaryTerm,
            Long.MAX_VALUE - generation,
            uuid
        );
    }

    /**
     * Parse generation from manifest file name.
     */
    public static long parseGeneration(String fileName) {
        String[] parts = fileName.split("__");
        if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid manifest file name: " + fileName);
        }
        return Long.MAX_VALUE - Long.parseLong(parts[2]);
    }

    /**
     * Parse primary term from manifest file name.
     */
    public static long parsePrimaryTerm(String fileName) {
        String[] parts = fileName.split("__");
        if (parts.length < 4) {
            throw new IllegalArgumentException("Invalid manifest file name: " + fileName);
        }
        return Long.MAX_VALUE - Long.parseLong(parts[1]);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVStateManifest that = (MVStateManifest) o;
        return primaryTerm == that.primaryTerm
            && generation == that.generation
            && maxSeqNo == that.maxSeqNo
            && defMetadataVersion == that.defMetadataVersion
            && definitionHash.equals(that.definitionHash)
            && files.equals(that.files);
    }

    @Override
    public int hashCode() {
        return Objects.hash(primaryTerm, generation, maxSeqNo, defMetadataVersion, definitionHash, files);
    }

    @Override
    public String toString() {
        return "MVStateManifest{term=" + primaryTerm + ", gen=" + generation + ", maxSeqNo=" + maxSeqNo + ", files=" + files.size() + "}";
    }

    /**
     * One file entry in the manifest.
     */
    public static class FileEntry implements Writeable {
        private final String name;
        private final long length;
        private final String checksum;

        public FileEntry(String name, long length, String checksum) {
            this.name = Objects.requireNonNull(name);
            this.length = length;
            this.checksum = Objects.requireNonNull(checksum);
        }

        public FileEntry(StreamInput in) throws IOException {
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

        public String name() {
            return name;
        }

        public long length() {
            return length;
        }

        public String checksum() {
            return checksum;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FileEntry fileEntry = (FileEntry) o;
            return length == fileEntry.length && name.equals(fileEntry.name) && checksum.equals(fileEntry.checksum);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, length, checksum);
        }

        @Override
        public String toString() {
            return "FileEntry{name='" + name + "', length=" + length + ", checksum='" + checksum + "'}";
        }
    }
}
