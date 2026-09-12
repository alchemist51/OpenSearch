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
            pub.leaderIndex()
        );
        Path dest2 = createTempDir().resolve("again.parquet");
        IOException e = expectThrows(IOException.class, () -> outbox.download(lying, dest2));
        assertTrue(e.getMessage(), e.getMessage().contains("size"));
        assertFalse(Files.exists(dest2));
    }
}
