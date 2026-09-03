/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Tests for {@link MVCheckpointMailbox}: deliver, consume, peek, coalesce,
 * fallback tracking, and concurrent-safety contracts.
 */
public class MVCheckpointMailboxTests extends OpenSearchTestCase {

    private MVCheckpointMailbox mailbox;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mailbox = new MVCheckpointMailbox();
    }

    public void testDeliverAndConsume() {
        MVCheckpointMailbox.PushedAdvert advert = advert(100L, 1L, 5L, List.of("a.parquet"));
        mailbox.deliver("target-idx", 0, advert);

        assertEquals(1, mailbox.pendingSlots());
        assertEquals(1L, mailbox.pushCount());

        MVCheckpointMailbox.PushedAdvert consumed = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(consumed);
        assertEquals(100L, consumed.maxSeqNo());
        assertEquals(List.of("a.parquet"), consumed.parquetFiles());

        // Consumed — slot is empty
        assertNull(mailbox.consume("target-idx", 0, "source-idx", 0));
        assertEquals(0, mailbox.pendingSlots());
        assertEquals(1L, mailbox.consumeCount());
    }

    public void testConsumeEmptyMailbox() {
        assertNull(mailbox.consume("target-idx", 0, "source-idx", 0));
    }

    public void testPeekDoesNotConsume() {
        MVCheckpointMailbox.PushedAdvert advert = advert(100L, 1L, 5L, List.of("a.parquet"));
        mailbox.deliver("target-idx", 0, advert);

        MVCheckpointMailbox.PushedAdvert peeked = mailbox.peek("target-idx", 0, "source-idx", 0);
        assertNotNull(peeked);
        assertEquals(100L, peeked.maxSeqNo());

        // Peek doesn't consume — still available
        assertEquals(1, mailbox.pendingSlots());
        MVCheckpointMailbox.PushedAdvert consumed = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(consumed);
    }

    public void testCoalesceNewerMaxSeqNo() {
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));
        mailbox.deliver("target-idx", 0, advert(200L, 1L, 6L, List.of("b.parquet")));

        MVCheckpointMailbox.PushedAdvert consumed = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(consumed);
        // Newer advert wins
        assertEquals(200L, consumed.maxSeqNo());
        assertEquals(List.of("b.parquet"), consumed.parquetFiles());
        assertEquals(2L, mailbox.pushCount());
    }

    public void testCoalesceOlderDropped() {
        mailbox.deliver("target-idx", 0, advert(200L, 1L, 6L, List.of("b.parquet")));
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));

        MVCheckpointMailbox.PushedAdvert consumed = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(consumed);
        // Newer advert (200) wins — older (100) is dropped
        assertEquals(200L, consumed.maxSeqNo());
    }

    public void testCoalesceSameMaxSeqNoHigherInfosVersion() {
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 7L, List.of("b.parquet")));

        MVCheckpointMailbox.PushedAdvert consumed = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(consumed);
        // Same maxSeqNo, higher infosVersion wins
        assertEquals(7L, consumed.infosVersion());
        assertEquals(List.of("b.parquet"), consumed.parquetFiles());
    }

    public void testMultipleSlots() {
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));
        mailbox.deliver("target-idx", 1, advert(200L, 1L, 6L, List.of("b.parquet")));
        mailbox.deliver("other-target", 0, advert(300L, 1L, 7L, List.of("c.parquet")));

        assertEquals(3, mailbox.pendingSlots());

        MVCheckpointMailbox.PushedAdvert a = mailbox.consume("target-idx", 0, "source-idx", 0);
        assertNotNull(a);
        assertEquals(100L, a.maxSeqNo());

        MVCheckpointMailbox.PushedAdvert b = mailbox.consume("target-idx", 1, "source-idx", 0);
        assertNotNull(b);
        assertEquals(200L, b.maxSeqNo());

        MVCheckpointMailbox.PushedAdvert c = mailbox.consume("other-target", 0, "source-idx", 0);
        assertNotNull(c);
        assertEquals(300L, c.maxSeqNo());

        assertEquals(0, mailbox.pendingSlots());
    }

    public void testFallbackCounter() {
        assertEquals(0L, mailbox.fallbackCount());
        mailbox.recordFallback();
        mailbox.recordFallback();
        assertEquals(2L, mailbox.fallbackCount());
    }

    public void testSingletonPattern() {
        MVCheckpointMailbox.setInstance(mailbox);
        assertSame(mailbox, MVCheckpointMailbox.instance());
        // Reset to avoid test pollution
        MVCheckpointMailbox.setInstance(null);
    }

    public void testLastConsumedWatermark_UnknownSlot() {
        assertEquals(-1L, mailbox.lastConsumedWatermark("target", 0, "source", 0));
    }

    public void testLastConsumedWatermark_UpdatedOnConsume() {
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));
        // Before consume — watermark unknown
        assertEquals(-1L, mailbox.lastConsumedWatermark("target-idx", 0, "source-idx", 0));

        mailbox.consume("target-idx", 0, "source-idx", 0);
        // After consume — watermark tracks the consumed maxSeqNo
        assertEquals(100L, mailbox.lastConsumedWatermark("target-idx", 0, "source-idx", 0));
    }

    public void testLastConsumedWatermark_MonotonicallyIncreasing() {
        mailbox.deliver("target-idx", 0, advert(100L, 1L, 5L, List.of("a.parquet")));
        mailbox.consume("target-idx", 0, "source-idx", 0);
        assertEquals(100L, mailbox.lastConsumedWatermark("target-idx", 0, "source-idx", 0));

        mailbox.deliver("target-idx", 0, advert(200L, 1L, 6L, List.of("b.parquet")));
        mailbox.consume("target-idx", 0, "source-idx", 0);
        assertEquals(200L, mailbox.lastConsumedWatermark("target-idx", 0, "source-idx", 0));

        // Deliver and consume an advert with a LOWER maxSeqNo — watermark should NOT regress
        mailbox.deliver("target-idx", 0, advert(150L, 1L, 7L, List.of("c.parquet")));
        mailbox.consume("target-idx", 0, "source-idx", 0);
        assertEquals(200L, mailbox.lastConsumedWatermark("target-idx", 0, "source-idx", 0));
    }

    public void testPushedAdvertWithSeqRanges() {
        MVCheckpointMailbox.PushedAdvert advert = new MVCheckpointMailbox.PushedAdvert(
            "source-idx", "source-uuid", 0,
            500L, 1L, 10L,
            List.of("a.parquet", "b.parquet"),
            List.of(1024L, 2048L),
            List.of(0L, 100L),
            List.of(99L, 500L),
            System.nanoTime()
        );
        assertEquals(List.of(0L, 100L), advert.fileMinSeqNos());
        assertEquals(List.of(99L, 500L), advert.fileMaxSeqNos());
    }

    public void testPushedAdvertLegacyConstructorDefaultsSeqRanges() {
        MVCheckpointMailbox.PushedAdvert advert = advert(100L, 1L, 5L, List.of("a.parquet", "b.parquet"));
        assertEquals(List.of(-1L, -1L), advert.fileMinSeqNos());
        assertEquals(List.of(-1L, -1L), advert.fileMaxSeqNos());
    }

    private static MVCheckpointMailbox.PushedAdvert advert(long maxSeqNo, long primaryTerm, long infosVersion, List<String> files) {
        return new MVCheckpointMailbox.PushedAdvert(
            "source-idx",
            "source-uuid",
            0,
            maxSeqNo,
            primaryTerm,
            infosVersion,
            files,
            files.stream().map(f -> -1L).toList(),
            System.nanoTime()
        );
    }
}
