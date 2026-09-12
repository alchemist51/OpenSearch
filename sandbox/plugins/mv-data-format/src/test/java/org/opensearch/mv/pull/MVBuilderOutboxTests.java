/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class MVBuilderOutboxTests extends OpenSearchTestCase {

    private Path root;
    private FsBlobStore store;
    private MVBuilderOutbox outbox;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = createTempDir();
        store = new FsBlobStore(8 * 1024, root, false);
        BlobContainer container = store.blobContainer(MVBuilderOutbox.path("src-uuid", 2, "mv_follower_a"));
        outbox = new MVBuilderOutbox(container);
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        super.tearDown();
    }

    private Path stateFile(String content) throws IOException {
        Path f = createTempDir().resolve("state.parquet");
        Files.write(f, content.getBytes(StandardCharsets.UTF_8));
        return f;
    }

    public void testEmptyOutboxHasNoLatest() throws IOException {
        assertNull(outbox.latest());
        assertTrue(outbox.since(-1L).isEmpty());
    }

    public void testPublishThenLatestRoundTrip() throws IOException {
        MVBuilderOutbox.Publication pub = outbox.publish(stateFile("abc"), -1L, 99L, 3L, 7L, 12L, -1L, "mv_leader");
        assertEquals(3L, pub.stateBytes());
        MVBuilderOutbox.Publication latest = outbox.latest();
        assertNotNull(latest);
        assertEquals(-1L, latest.fromExclusive());
        assertEquals(99L, latest.toInclusive());
        assertEquals(3L, latest.primaryTerm());
        assertEquals(7L, latest.infosVersion());
        assertEquals(12L, latest.rows());
        assertEquals(3L, latest.stateBytes());
        assertEquals(-1L, latest.prevToInclusive());
        assertEquals("mv_leader", latest.leaderIndex());
        assertTrue(latest.publishedEpochMs() > 0L);
        assertEquals("state-00000000000000000099.parquet", latest.stateBlob());
        assertEquals("pub-00000000000000000099.json", latest.manifestBlob());
        // the per-publication manifest is readable on its own
        assertEquals(99L, outbox.read(99L).toInclusive());
        assertNull(outbox.read(42L));
    }

    public void testSinceWalksChainOldestFirstAndStopsAtWatermark() throws IOException {
        outbox.publish(stateFile("a"), -1L, 10L, 1L, 1L, 1L, -1L, "l");
        outbox.publish(stateFile("bb"), 10L, 20L, 1L, 2L, 2L, 10L, "l");
        outbox.publish(stateFile("ccc"), 20L, 30L, 1L, 3L, 3L, 20L, "l");
        outbox.publish(stateFile("dddd"), 30L, 40L, 1L, 4L, 4L, 30L, "l");

        List<MVBuilderOutbox.Publication> all = outbox.since(-1L);
        assertEquals(4, all.size());
        assertEquals(10L, all.get(0).toInclusive());
        assertEquals(40L, all.get(3).toInclusive());

        List<MVBuilderOutbox.Publication> tail = outbox.since(20L);
        assertEquals(2, tail.size());
        assertEquals(30L, tail.get(0).toInclusive());
        assertEquals(40L, tail.get(1).toInclusive());

        assertEquals(1, outbox.since(30L).size());
        assertTrue(outbox.since(40L).isEmpty());
        assertTrue(outbox.since(41L).isEmpty());
    }

    public void testBrokenChainIsRefused() throws IOException {
        outbox.publish(stateFile("a"), -1L, 10L, 1L, 1L, 1L, -1L, "l");
        outbox.publish(stateFile("bb"), 10L, 20L, 1L, 2L, 2L, 10L, "l");
        // publication 30 claims prev=25, which was never published
        outbox.publish(stateFile("ccc"), 25L, 30L, 1L, 3L, 3L, 25L, "l");
        IOException e = expectThrows(IOException.class, () -> outbox.since(-1L));
        assertTrue(e.getMessage(), e.getMessage().contains("broken chain"));
        // but a follower already at 25 does not need the missing link
        assertEquals(1, outbox.since(25L).size());
    }

    public void testDownloadVerifiesSize() throws IOException {
        MVBuilderOutbox.Publication pub = outbox.publish(stateFile("hello"), -1L, 5L, 1L, 1L, 1L, -1L, "l");
        Path dest = createTempDir().resolve(pub.stateBlob());
        outbox.download(pub, dest);
        assertEquals("hello", Files.readString(dest));

        MVBuilderOutbox.Publication lying = new MVBuilderOutbox.Publication(
            pub.fromExclusive(),
            pub.toInclusive(),
            pub.primaryTerm(),
            pub.infosVersion(),
            pub.rows(),
            pub.stateBytes() + 1,
            pub.prevToInclusive(),
            pub.publishedEpochMs(),
            pub.leaderIndex(),
            pub.crc32()
        );
        Path dest2 = createTempDir().resolve("again.parquet");
        IOException e = expectThrows(IOException.class, () -> outbox.download(lying, dest2));
        assertTrue(e.getMessage(), e.getMessage().contains("size"));
        assertFalse(Files.exists(dest2));

        // right size, wrong checksum -> refused as well
        MVBuilderOutbox.Publication corrupt = new MVBuilderOutbox.Publication(
            pub.fromExclusive(),
            pub.toInclusive(),
            pub.primaryTerm(),
            pub.infosVersion(),
            pub.rows(),
            pub.stateBytes(),
            pub.prevToInclusive(),
            pub.publishedEpochMs(),
            pub.leaderIndex(),
            pub.crc32() ^ 1L
        );
        Path dest3 = createTempDir().resolve("corrupt.parquet");
        IOException e2 = expectThrows(IOException.class, () -> outbox.download(corrupt, dest3));
        assertTrue(e2.getMessage(), e2.getMessage().contains("crc32"));
        assertFalse(Files.exists(dest3));
    }

    public void testPublishRecordsCrc32AndManifestCarriesIt() throws IOException {
        Path f = stateFile("checksum me");
        MVBuilderOutbox.Publication pub = outbox.publish(f, -1L, 7L, 1L, 1L, 2L, -1L, "l");
        assertEquals(MVBuilderOutbox.crc32(f), pub.crc32());
        assertTrue(pub.crc32() != 0L);
        assertEquals(pub.crc32(), outbox.latest().crc32());
        assertEquals(pub.crc32(), outbox.read(7L).crc32());
    }

    public void testTrimDeletesStateAndManifestButKeepsLatest() throws IOException {
        MVBuilderOutbox.Publication a = outbox.publish(stateFile("a"), -1L, 10L, 1L, 1L, 1L, -1L, "l");
        MVBuilderOutbox.Publication b = outbox.publish(stateFile("bb"), 10L, 20L, 1L, 2L, 2L, 10L, "l");
        assertEquals(5, outbox.listBlobs().size()); // 2 states + 2 manifests + latest
        outbox.trim(a);
        List<String> left = outbox.listBlobs();
        assertEquals(3, left.size());
        assertFalse(left.contains(a.stateBlob()));
        assertFalse(left.contains(a.manifestBlob()));
        assertTrue(left.contains(b.stateBlob()));
        assertTrue(left.contains(MVBuilderOutbox.LATEST));
        // a follower already at the newest watermark still reads "nothing new"
        assertTrue(outbox.since(20L).isEmpty());
        // a follower behind the trimmed range cannot be served from the outbox any more (retention exceeded)
        IOException e = expectThrows(IOException.class, () -> outbox.since(-1L));
        assertTrue(e.getMessage(), e.getMessage().contains("broken chain"));
        // trimming twice is harmless
        outbox.trim(a);
        assertEquals(3, outbox.listBlobs().size());
    }

    public void testPublicationWireRoundTrip() throws IOException {
        MVBuilderOutbox.Publication pub = new MVBuilderOutbox.Publication(
            -1L,
            99L,
            3L,
            7L,
            12L,
            3L,
            -1L,
            1_700_000_000_000L,
            "mv_leader",
            0xCAFEL
        );
        try (org.opensearch.common.io.stream.BytesStreamOutput out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
            pub.writeTo(out);
            try (org.opensearch.core.common.io.stream.StreamInput in = out.bytes().streamInput()) {
                assertEquals(pub, MVBuilderOutbox.Publication.readFrom(in));
            }
        }
    }
}
