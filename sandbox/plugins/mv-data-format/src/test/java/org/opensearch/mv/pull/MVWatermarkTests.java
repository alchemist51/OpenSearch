/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link MVWatermark} encode/decode round-trip, key format,
 * EMPTY sentinel, and edge cases (malformed values, missing entries).
 */
public class MVWatermarkTests extends OpenSearchTestCase {

    // ── Encode / Decode round-trip ────────────────────────────────────────

    public void testEncodeDecodeRoundTrip() {
        MVWatermark original = new MVWatermark(5L, 42L, 100L);
        String encoded = original.encode();
        MVWatermark decoded = MVWatermark.decode(encoded);

        assertEquals(original.primaryTerm(), decoded.primaryTerm());
        assertEquals(original.seqNo(), decoded.seqNo());
        assertEquals(original.generation(), decoded.generation());
    }

    public void testEncodeFormat() {
        MVWatermark wm = new MVWatermark(1L, 200L, 3L);
        assertEquals("1:200:3", wm.encode());
    }

    public void testDecodeValidString() {
        MVWatermark wm = MVWatermark.decode("7:999:42");
        assertEquals(7L, wm.primaryTerm());
        assertEquals(999L, wm.seqNo());
        assertEquals(42L, wm.generation());
    }

    // ── EMPTY sentinel ────────────────────────────────────────────────────

    public void testEmptyWatermarkValues() {
        assertEquals(-1L, MVWatermark.EMPTY.primaryTerm());
        assertEquals(-1L, MVWatermark.EMPTY.seqNo());
        assertEquals(-1L, MVWatermark.EMPTY.generation());
    }

    public void testEmptyWatermarkEncodesCorrectly() {
        String encoded = MVWatermark.EMPTY.encode();
        assertEquals("-1:-1:-1", encoded);
    }

    public void testEmptyWatermarkDecodeRoundTrip() {
        MVWatermark decoded = MVWatermark.decode(MVWatermark.EMPTY.encode());
        assertEquals(MVWatermark.EMPTY.primaryTerm(), decoded.primaryTerm());
        assertEquals(MVWatermark.EMPTY.seqNo(), decoded.seqNo());
        assertEquals(MVWatermark.EMPTY.generation(), decoded.generation());
    }

    // ── Key format ────────────────────────────────────────────────────────

    public void testKeyFormat() {
        assertEquals("mv.wm.0", MVWatermark.key(0));
        assertEquals("mv.wm.5", MVWatermark.key(5));
        assertEquals("mv.wm.42", MVWatermark.key(42));
    }

    public void testKeyPrefix() {
        assertEquals("mv.wm.", MVWatermark.KEY_PREFIX);
        assertTrue(MVWatermark.key(3).startsWith(MVWatermark.KEY_PREFIX));
    }

    // ── fromCommitUserData ────────────────────────────────────────────────

    public void testFromCommitUserDataReturnsWatermarkWhenPresent() {
        Map<String, String> userData = new HashMap<>();
        userData.put("mv.wm.0", "3:500:10");

        MVWatermark wm = MVWatermark.fromCommitUserData(userData, 0);
        assertEquals(3L, wm.primaryTerm());
        assertEquals(500L, wm.seqNo());
        assertEquals(10L, wm.generation());
    }

    public void testFromCommitUserDataReturnsEmptyWhenMissing() {
        Map<String, String> userData = new HashMap<>();
        // No watermark entry for shard 0

        MVWatermark wm = MVWatermark.fromCommitUserData(userData, 0);
        assertEquals(MVWatermark.EMPTY.primaryTerm(), wm.primaryTerm());
        assertEquals(MVWatermark.EMPTY.seqNo(), wm.seqNo());
        assertEquals(MVWatermark.EMPTY.generation(), wm.generation());
    }

    public void testFromCommitUserDataDifferentShardIds() {
        Map<String, String> userData = new HashMap<>();
        userData.put("mv.wm.0", "1:100:5");
        userData.put("mv.wm.1", "2:200:10");

        MVWatermark wm0 = MVWatermark.fromCommitUserData(userData, 0);
        MVWatermark wm1 = MVWatermark.fromCommitUserData(userData, 1);

        assertEquals(100L, wm0.seqNo());
        assertEquals(200L, wm1.seqNo());
    }

    // ── Error cases ───────────────────────────────────────────────────────

    public void testDecodeMalformedStringThrows() {
        expectThrows(IllegalStateException.class, () -> MVWatermark.decode("bad"));
        expectThrows(IllegalStateException.class, () -> MVWatermark.decode("1:2"));
        expectThrows(IllegalStateException.class, () -> MVWatermark.decode("1:2:3:4"));
    }

    public void testDecodeNullThrows() {
        expectThrows(NullPointerException.class, () -> MVWatermark.decode(null));
    }

    public void testDecodeNonNumericThrows() {
        expectThrows(NumberFormatException.class, () -> MVWatermark.decode("a:b:c"));
    }

    // ── toString ──────────────────────────────────────────────────────────

    public void testToStringContainsAllFields() {
        MVWatermark wm = new MVWatermark(2L, 500L, 8L);
        String str = wm.toString();
        assertTrue(str.contains("term=2"));
        assertTrue(str.contains("seqNo=500"));
        assertTrue(str.contains("gen=8"));
    }

    // ── Large values ──────────────────────────────────────────────────────

    public void testLargeValuesRoundTrip() {
        MVWatermark wm = new MVWatermark(Long.MAX_VALUE, Long.MAX_VALUE - 1, Long.MAX_VALUE - 2);
        MVWatermark decoded = MVWatermark.decode(wm.encode());
        assertEquals(Long.MAX_VALUE, decoded.primaryTerm());
        assertEquals(Long.MAX_VALUE - 1, decoded.seqNo());
        assertEquals(Long.MAX_VALUE - 2, decoded.generation());
    }
}
