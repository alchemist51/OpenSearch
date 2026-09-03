/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.mv.MVFileMetadata;
import org.opensearch.mv.MVReplicationCheckpoint;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Unit tests for {@link MVCheckpointMailbox} (now using {@link MVReplicationCheckpoint})
 * and {@link MVReplicationCheckpoint#isAheadOf} ordering (term-first, failover-correct).
 */
public class MVCheckpointMailboxTests extends OpenSearchTestCase {

    // ── Basic mailbox operations ─────────────────────────────────────────

    public void testConsumeReturnsNullWhenEmpty() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        assertNull(mailbox.consume("target", 0, "source", 0));
    }

    public void testDeliverAndConsume() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        MVReplicationCheckpoint cp = checkpoint("source", 0, 1L, 100L, 5L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, -1L)));
        mailbox.deliver("target", 0, cp);

        MVReplicationCheckpoint consumed = mailbox.consume("target", 0, "source", 0);
        assertNotNull(consumed);
        assertEquals(100L, consumed.maxSeqNo());
        assertEquals(1, consumed.fileMetadata().size());

        // Second consume should return null (consumed)
        assertNull(mailbox.consume("target", 0, "source", 0));
    }

    public void testCoalesceKeepsNewerCheckpoint() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        MVReplicationCheckpoint older = checkpoint("source", 0, 1L, 100L, 5L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, -1L)));
        MVReplicationCheckpoint newer = checkpoint("source", 0, 1L, 200L, 10L,
            Map.of("b.parquet", new MVFileMetadata(2048L, 100L, 200L, -1L),
                   "c.parquet", new MVFileMetadata(512L, 200L, 250L, -1L)));
        mailbox.deliver("target", 0, older);
        mailbox.deliver("target", 0, newer);

        MVReplicationCheckpoint consumed = mailbox.consume("target", 0, "source", 0);
        assertNotNull(consumed);
        assertEquals(200L, consumed.maxSeqNo());
        assertEquals(2, consumed.fileMetadata().size());
    }

    public void testPeekDoesNotConsume() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        MVReplicationCheckpoint cp = checkpoint("source", 0, 1L, 100L, 5L, Map.of());
        mailbox.deliver("target", 0, cp);

        MVReplicationCheckpoint peeked = mailbox.peek("target", 0, "source", 0);
        assertNotNull(peeked);
        assertEquals(100L, peeked.maxSeqNo());

        MVReplicationCheckpoint consumed = mailbox.consume("target", 0, "source", 0);
        assertNotNull(consumed);
        assertEquals(100L, consumed.maxSeqNo());
    }

    public void testFallbackCounter() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        assertEquals(0L, mailbox.fallbackCount());
        mailbox.recordFallback();
        mailbox.recordFallback();
        assertEquals(2L, mailbox.fallbackCount());
    }

    public void testLastConsumedWatermark() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        assertEquals(-1L, mailbox.lastConsumedWatermark("target", 0, "source", 0));

        mailbox.deliver("target", 0, checkpoint("source", 0, 1L, 100L, 5L, Map.of()));
        mailbox.consume("target", 0, "source", 0);
        assertEquals(100L, mailbox.lastConsumedWatermark("target", 0, "source", 0));

        mailbox.deliver("target", 0, checkpoint("source", 0, 1L, 200L, 10L, Map.of()));
        mailbox.consume("target", 0, "source", 0);
        assertEquals(200L, mailbox.lastConsumedWatermark("target", 0, "source", 0));
    }

    public void testPerSlotIsolation() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        mailbox.deliver("target-a", 0, checkpoint("source-a", 0, 1L, 100L, 5L, Map.of()));
        mailbox.deliver("target-b", 0, checkpoint("source-b", 0, 1L, 200L, 10L, Map.of()));

        MVReplicationCheckpoint consumedA = mailbox.consume("target-a", 0, "source-a", 0);
        assertNotNull(consumedA);
        assertEquals(100L, consumedA.maxSeqNo());

        MVReplicationCheckpoint consumedB = mailbox.consume("target-b", 0, "source-b", 0);
        assertNotNull(consumedB);
        assertEquals(200L, consumedB.maxSeqNo());
    }

    // ── isAheadOf ordering tests (term-first, failover-correct) ──────────

    public void testIsAheadOf_HigherTermWins() {
        MVReplicationCheckpoint termOne = checkpoint("src", 0, 1L, 1000L, 50L, Map.of());
        MVReplicationCheckpoint termTwo = checkpoint("src", 0, 2L, 100L, 5L, Map.of());
        // Term 2 is ahead of term 1 even though seqNo is lower
        assertTrue(termTwo.isAheadOf(termOne));
        assertFalse(termOne.isAheadOf(termTwo));
    }

    public void testIsAheadOf_SameTermHigherSeqNoWins() {
        MVReplicationCheckpoint lower = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        MVReplicationCheckpoint higher = checkpoint("src", 0, 1L, 200L, 10L, Map.of());
        assertTrue(higher.isAheadOf(lower));
        assertFalse(lower.isAheadOf(higher));
    }

    public void testIsAheadOf_SameTermSameSeqNoHigherInfosVersionWins() {
        MVReplicationCheckpoint lower = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        MVReplicationCheckpoint higher = checkpoint("src", 0, 1L, 100L, 20L, Map.of());
        assertTrue(higher.isAheadOf(lower));
        assertFalse(lower.isAheadOf(higher));
    }

    public void testIsAheadOf_EqualCheckpoints() {
        MVReplicationCheckpoint a = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        MVReplicationCheckpoint b = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        assertFalse(a.isAheadOf(b));
        assertFalse(b.isAheadOf(a));
    }

    public void testIsAheadOf_NullSafe() {
        MVReplicationCheckpoint cp = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        assertTrue(cp.isAheadOf(null));
    }

    public void testIsAheadOf_EmptySafe() {
        MVReplicationCheckpoint cp = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        MVReplicationCheckpoint empty = MVReplicationCheckpoint.empty("src", 0);
        assertTrue(cp.isAheadOf(empty));
        assertFalse(empty.isAheadOf(cp));
    }

    public void testIsAheadOf_EmptyVsEmpty() {
        MVReplicationCheckpoint e1 = MVReplicationCheckpoint.empty("src", 0);
        MVReplicationCheckpoint e2 = MVReplicationCheckpoint.empty("src", 0);
        assertFalse(e1.isAheadOf(e2));
        assertFalse(e2.isAheadOf(e1));
    }

    public void testIsAheadOf_EmptyVsNull() {
        MVReplicationCheckpoint empty = MVReplicationCheckpoint.empty("src", 0);
        // EMPTY is not ahead of null: both are sentinel values, EMPTY.isEmpty() is true
        assertFalse(empty.isAheadOf(null));
    }

    // ── Coalesce across failover (term-first mailbox ordering fix) ───────

    public void testCoalesceAcrossFailover() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        // Old-term advert with high seqNo
        MVReplicationCheckpoint oldTerm = checkpoint("source", 0, 1L, 5000L, 100L, Map.of());
        // New-term advert with low seqNo (after failover, new primary starts from scratch)
        MVReplicationCheckpoint newTerm = checkpoint("source", 0, 2L, 50L, 5L, Map.of());

        mailbox.deliver("target", 0, oldTerm);
        mailbox.deliver("target", 0, newTerm);

        MVReplicationCheckpoint consumed = mailbox.consume("target", 0, "source", 0);
        assertNotNull(consumed);
        // New term must win despite lower seqNo — failover correctness
        assertEquals(2L, consumed.primaryTerm());
        assertEquals(50L, consumed.maxSeqNo());
    }

    public void testCoalesceOldTermDoesNotSupersede() {
        MVCheckpointMailbox mailbox = new MVCheckpointMailbox();
        // New-term advert arrives first
        MVReplicationCheckpoint newTerm = checkpoint("source", 0, 2L, 50L, 5L, Map.of());
        // Delayed old-term advert arrives after (network reordering)
        MVReplicationCheckpoint oldTerm = checkpoint("source", 0, 1L, 5000L, 100L, Map.of());

        mailbox.deliver("target", 0, newTerm);
        mailbox.deliver("target", 0, oldTerm);

        MVReplicationCheckpoint consumed = mailbox.consume("target", 0, "source", 0);
        assertNotNull(consumed);
        // New term must still be kept — old-term advert must not supersede
        assertEquals(2L, consumed.primaryTerm());
        assertEquals(50L, consumed.maxSeqNo());
    }

    // ── compareTo consistency ────────────────────────────────────────────

    public void testCompareTo() {
        MVReplicationCheckpoint a = checkpoint("src", 0, 1L, 100L, 10L, Map.of());
        MVReplicationCheckpoint b = checkpoint("src", 0, 1L, 200L, 10L, Map.of());
        // b is ahead → compareTo returns negative for b (sorted first)
        assertTrue(b.compareTo(a) < 0);
        assertTrue(a.compareTo(b) > 0);
        assertEquals(0, a.compareTo(checkpoint("src", 0, 1L, 100L, 10L, Map.of())));
    }

    // ── Equality: positional identity, NOT the map ───────────────────────

    public void testEqualityIgnoresFileMetadata() {
        MVReplicationCheckpoint a = checkpoint("src", 0, 1L, 100L, 10L,
            Map.of("a.parquet", new MVFileMetadata(1024L, 0L, 100L, 111L)));
        MVReplicationCheckpoint b = checkpoint("src", 0, 1L, 100L, 10L,
            Map.of("b.parquet", new MVFileMetadata(9999L, 50L, 200L, 222L)));
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    // ── CRC verification helpers ─────────────────────────────────────────

    public void testCrc32GoodFile() throws IOException {
        Path dir = createTempDir();
        Path file = dir.resolve("test.parquet");
        byte[] content = "hello world parquet data".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(content);
        long expectedCrc = crc.getValue();

        try (OutputStream os = Files.newOutputStream(file)) {
            os.write(content);
        }

        // Verify CRC matches
        CRC32 verify = new CRC32();
        verify.update(Files.readAllBytes(file));
        assertEquals(expectedCrc, verify.getValue());
    }

    public void testCrc32CorruptedFile() throws IOException {
        Path dir = createTempDir();
        Path file = dir.resolve("test.parquet");
        byte[] content = "original content".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        CRC32 crc = new CRC32();
        crc.update(content);
        long originalCrc = crc.getValue();

        // Write corrupted content
        byte[] corrupted = "corrupted content".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        try (OutputStream os = Files.newOutputStream(file)) {
            os.write(corrupted);
        }

        // CRC should NOT match
        CRC32 verify = new CRC32();
        verify.update(Files.readAllBytes(file));
        assertNotEquals(originalCrc, verify.getValue());
    }

    // ── helpers ──────────────────────────────────────────────────────────

    private static MVReplicationCheckpoint checkpoint(
        String sourceIndex, int sourceShard, long primaryTerm, long maxSeqNo, long infosVersion,
        Map<String, MVFileMetadata> fileMetadata
    ) {
        return new MVReplicationCheckpoint(
            sourceIndex, sourceShard, primaryTerm, maxSeqNo, infosVersion,
            fileMetadata, System.currentTimeMillis()
        );
    }
}
