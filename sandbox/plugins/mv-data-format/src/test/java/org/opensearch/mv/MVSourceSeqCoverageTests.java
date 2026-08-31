/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class MVSourceSeqCoverageTests extends OpenSearchTestCase {

    public void testNonContiguousChunksAdvanceFloorAndRetainExactAboveFloorClaim() {
        MVSourceSeqCoverage c1 = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 2L, 3L, 5L));
        MVSourceSeqCoverage cx = MVSourceSeqCoverage.ofSeqNos(List.of(8L, 10L, 20L));
        MVSourceSeqCoverage c2 = MVSourceSeqCoverage.ofSeqNos(List.of(1L, 4L, 6L, 7L));

        MVSourceSeqCoverage claim = c1.union(cx).union(c2);

        assertEquals(8L, claim.floor());
        assertEquals(List.of(new MVSourceSeqCoverage.Range(10L, 10L), new MVSourceSeqCoverage.Range(20L, 20L)), claim.aboveFloor());
        assertTrue(claim.contains(0L));
        assertTrue(claim.contains(8L));
        assertTrue(claim.contains(10L));
        assertFalse(claim.contains(9L));
    }

    public void testAboveFloorRangesSelfTruncateAsHolesArrive() {
        MVSourceSeqCoverage claim = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 2L, 3L, 5L));
        assertEquals(0L, claim.floor());
        assertEquals(List.of(new MVSourceSeqCoverage.Range(2L, 3L), new MVSourceSeqCoverage.Range(5L, 5L)), claim.aboveFloor());

        claim = claim.union(MVSourceSeqCoverage.ofSeqNos(List.of(1L, 4L)));

        assertEquals(5L, claim.floor());
        assertTrue(claim.aboveFloor().isEmpty());
    }

    public void testComplementReturnsOnlyMissingRuns() {
        MVSourceSeqCoverage claim = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 1L, 3L, 4L, 7L, 9L));

        assertEquals(
            List.of(
                new MVSourceSeqCoverage.Range(2L, 2L),
                new MVSourceSeqCoverage.Range(5L, 6L),
                new MVSourceSeqCoverage.Range(8L, 8L),
                new MVSourceSeqCoverage.Range(10L, 12L)
            ),
            claim.missingThrough(12L)
        );
    }

    public void testThroughRetainsOnlyExactClaimAtOrBelowCommittedBound() {
        MVSourceSeqCoverage claim = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 1L, 3L, 4L, 7L, 8L, 10L));

        assertEquals(MVSourceSeqCoverage.EMPTY, claim.through(-1L));
        assertEquals(MVSourceSeqCoverage.contiguous(1L), claim.through(2L));
        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(0L, 1L, 3L, 4L, 7L)), claim.through(7L));
        assertEquals(claim, claim.through(100L));
    }

    public void testIntersectionAndSubtractionPreserveSparseExactRanges() {
        MVSourceSeqCoverage missing = MVSourceSeqCoverage.ofRanges(
            List.of(new MVSourceSeqCoverage.Range(0L, 3L), new MVSourceSeqCoverage.Range(5L, 8L))
        );
        MVSourceSeqCoverage knownNoOps = MVSourceSeqCoverage.ofSeqNos(List.of(1L, 2L, 5L, 7L, 10L));

        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(1L, 2L, 5L, 7L)), missing.intersection(knownNoOps));
        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(0L, 3L, 6L, 8L)), missing.subtract(knownNoOps));
        assertEquals(missing, missing.subtract(MVSourceSeqCoverage.EMPTY));
        assertEquals(MVSourceSeqCoverage.EMPTY, missing.intersection(MVSourceSeqCoverage.EMPTY));
        assertEquals(MVSourceSeqCoverage.EMPTY, missing.subtract(missing));
    }

    public void testStringAndWireRoundTrip() throws Exception {
        MVSourceSeqCoverage expected = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 1L, 4L, 5L, 8L, 10L, 11L));

        assertEquals(expected, MVSourceSeqCoverage.decode(expected.encode()));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            expected.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(expected, MVSourceSeqCoverage.readFrom(in));
            }
        }
    }

    public void testLegacyFloorDecodeAndMalformedInput() {
        assertEquals(MVSourceSeqCoverage.contiguous(41L), MVSourceSeqCoverage.decode("41"));
        assertEquals(MVSourceSeqCoverage.EMPTY, MVSourceSeqCoverage.decode("not-a-claim"));
    }
}
