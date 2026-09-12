/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Builder-shard emulation: the per-follower <em>outbox</em> in the remote
 * repository through which a leader (builder) hands folded MV state to a
 * hydrating follower target.
 *
 * <p>Layout under {@code mv_builder/<sourceIndexUUID>/<sourceShard>/<followerIndex>/}:
 * <ul>
 *   <li>{@code state-<to>.parquet} — the folded state for source seq-no range {@code (from, to]}</li>
 *   <li>{@code pub-<to>.json} — the publication manifest for that range, carrying {@code prev} (the
 *       previous publication's {@code to}) so a follower that fell behind can walk the chain</li>
 *   <li>{@code latest.json} — a copy of the newest manifest, overwritten atomically on every publish;
 *       one GET per follower poll in steady state</li>
 * </ul>
 * Nothing is ever trimmed by this class (emulation scope).
 */
public final class MVBuilderOutbox {

    public static final String ROOT = "mv_builder";
    public static final String LATEST = "latest.json";
    static final int MAX_CHAIN = 4096;

    /** One publication: the state file for source range {@code (fromExclusive, toInclusive]}. */
    public record Publication(long fromExclusive, long toInclusive, long primaryTerm, long infosVersion, long rows, long stateBytes,
        long prevToInclusive, long publishedEpochMs, String leaderIndex) {
        public String stateBlob() {
            return stateBlobFor(toInclusive);
        }

        public String manifestBlob() {
            return manifestBlobFor(toInclusive);
        }

        static String stateBlobFor(long toInclusive) {
            return "state-" + pad(toInclusive) + ".parquet";
        }

        static String manifestBlobFor(long toInclusive) {
            return "pub-" + pad(toInclusive) + ".json";
        }

        private static String pad(long v) {
            return String.format(java.util.Locale.ROOT, "%020d", v);
        }

        byte[] toJson() throws IOException {
            try (XContentBuilder b = XContentFactory.jsonBuilder()) {
                b.startObject();
                b.field("from", fromExclusive);
                b.field("to", toInclusive);
                b.field("primary_term", primaryTerm);
                b.field("infos_version", infosVersion);
                b.field("rows", rows);
                b.field("state_bytes", stateBytes);
                b.field("prev", prevToInclusive);
                b.field("published_epoch_ms", publishedEpochMs);
                b.field("leader", leaderIndex);
                b.endObject();
                return BytesReference.toBytes(BytesReference.bytes(b));
            }
        }

        static Publication fromJson(InputStream in) throws IOException {
            try (
                XContentParser p = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE,
                    in
                )
            ) {
                Map<String, Object> m = p.map();
                return new Publication(
                    num(m, "from"),
                    num(m, "to"),
                    num(m, "primary_term"),
                    num(m, "infos_version"),
                    num(m, "rows"),
                    num(m, "state_bytes"),
                    num(m, "prev"),
                    num(m, "published_epoch_ms"),
                    String.valueOf(m.get("leader"))
                );
            }
        }

        private static long num(Map<String, Object> m, String key) throws IOException {
            Object v = m.get(key);
            if (v instanceof Number n) {
                return n.longValue();
            }
            throw new IOException("mv_builder outbox manifest: missing or non-numeric field [" + key + "]");
        }
    }

    private final BlobContainer container;

    public MVBuilderOutbox(BlobContainer container) {
        this.container = container;
    }

    /** The blob path of one follower's outbox. */
    public static BlobPath path(String sourceIndexUuid, int sourceShardId, String followerIndex) {
        return BlobPath.cleanPath().add(ROOT).add(sourceIndexUuid).add(Integer.toString(sourceShardId)).add(followerIndex);
    }

    /** Open the outbox for one follower inside the given repository. */
    public static MVBuilderOutbox open(
        Supplier<RepositoriesService> repositoriesService,
        String repositoryName,
        String sourceIndexUuid,
        int sourceShardId,
        String followerIndex
    ) {
        Repository repository = repositoriesService.get().repository(repositoryName);
        if (repository instanceof BlobStoreRepository == false) {
            throw new IllegalStateException("mv_builder outbox: repository [" + repositoryName + "] is not a blob store repository");
        }
        BlobContainer c = ((BlobStoreRepository) repository).blobStore().blobContainer(path(sourceIndexUuid, sourceShardId, followerIndex));
        return new MVBuilderOutbox(c);
    }

    // ── writer side (leader) ────────────────────────────────────────────────

    /**
     * Upload {@code stateFile} as the publication for {@code (fromExclusive, toInclusive]}
     * and advance {@code latest.json}. Order: state, manifest, latest — a reader that
     * sees a manifest can always fetch its state.
     */
    public Publication publish(
        Path stateFile,
        long fromExclusive,
        long toInclusive,
        long primaryTerm,
        long infosVersion,
        long rows,
        long prevToInclusive,
        String leaderIndex
    ) throws IOException {
        long size = Files.size(stateFile);
        Publication pub = new Publication(
            fromExclusive,
            toInclusive,
            primaryTerm,
            infosVersion,
            rows,
            size,
            prevToInclusive,
            System.currentTimeMillis(),
            leaderIndex
        );
        try (InputStream in = Files.newInputStream(stateFile)) {
            container.writeBlob(pub.stateBlob(), in, size, false);
        }
        byte[] json = pub.toJson();
        container.writeBlobAtomic(pub.manifestBlob(), new java.io.ByteArrayInputStream(json), json.length, false);
        container.writeBlobAtomic(LATEST, new java.io.ByteArrayInputStream(json), json.length, false);
        return pub;
    }

    // ── reader side (follower) ──────────────────────────────────────────────

    /** The newest publication, or {@code null} when the leader has published nothing yet. */
    public Publication latest() throws IOException {
        return readManifest(LATEST);
    }

    /** The publication whose range ends at {@code toInclusive}, or {@code null} if absent. */
    public Publication read(long toInclusive) throws IOException {
        return readManifest(Publication.manifestBlobFor(toInclusive));
    }

    /**
     * All publications with {@code to > sinceToInclusive}, oldest first, found by
     * walking the {@code prev} chain from {@code latest.json}. Throws when the
     * chain is broken (a manifest is missing) — a gap must never be skipped.
     */
    public List<Publication> since(long sinceToInclusive) throws IOException {
        Publication p = latest();
        if (p == null || p.toInclusive() <= sinceToInclusive) {
            return Collections.emptyList();
        }
        List<Publication> chain = new ArrayList<>();
        chain.add(p);
        while (p.prevToInclusive() > sinceToInclusive) {
            if (chain.size() >= MAX_CHAIN) {
                throw new IOException("mv_builder outbox: publication chain longer than " + MAX_CHAIN + " since " + sinceToInclusive);
            }
            Publication prev = read(p.prevToInclusive());
            if (prev == null) {
                throw new IOException(
                    "mv_builder outbox: broken chain — publication to="
                        + p.prevToInclusive()
                        + " referenced by to="
                        + p.toInclusive()
                        + " is missing"
                );
            }
            chain.add(prev);
            p = prev;
        }
        Collections.reverse(chain);
        return chain;
    }

    /** Download the state file of {@code pub} to {@code dest}, verifying its size. */
    public void download(Publication pub, Path dest) throws IOException {
        Path tmp = dest.resolveSibling(dest.getFileName() + ".part");
        try (InputStream in = container.readBlob(pub.stateBlob())) {
            Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING);
        }
        long size = Files.size(tmp);
        if (size != pub.stateBytes()) {
            Files.deleteIfExists(tmp);
            throw new IOException("mv_builder outbox: state [" + pub.stateBlob() + "] size " + size + " != manifest " + pub.stateBytes());
        }
        Files.move(tmp, dest, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private Publication readManifest(String blob) throws IOException {
        try (InputStream in = container.readBlob(blob)) {
            return Publication.fromJson(in);
        } catch (NoSuchFileException e) {
            return null;
        }
    }
}
