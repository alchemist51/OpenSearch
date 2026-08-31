/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class MVTargetCursorLedgerTests extends OpenSearchTestCase {

    private static final String TARGET = "mv_target";
    private static final String SOURCE = "source";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MVTargetCursorLedger.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        MVTargetCursorLedger.clearForTests();
        super.tearDown();
    }

    public void testAppliedBatchIsNotDurableUntilCommitSucceeds() {
        MVTargetCursorLedger.record(TARGET, 0, SOURCE, 0, 7L, 41L, 41L, 3, 3);

        assertEquals(MVTargetCursorLedger.Cursor.NONE, MVTargetCursorLedger.committed(TARGET, 0, SOURCE, 0));
        assertFalse(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));

        Map<String, MVTargetCursorLedger.Cursor> candidates = MVTargetCursorLedger.commitCandidatesForTarget(TARGET, 0);
        MVTargetCursorLedger.Cursor candidate = candidates.get(SOURCE + ".0");
        assertEquals(new MVTargetCursorLedger.Cursor(7L, 41L), candidate);

        MVTargetCursorLedger.markCommitted(TARGET, 0, SOURCE, 0, candidate);

        assertEquals(candidate, MVTargetCursorLedger.committed(TARGET, 0, SOURCE, 0));
        assertTrue(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));
    }

    public void testNewerAppliedBatchKeepsStateMergeClosed() {
        MVTargetCursorLedger.Cursor first = new MVTargetCursorLedger.Cursor(3L, 10L);
        MVTargetCursorLedger.seed(TARGET, 0, SOURCE, 0, first);
        MVTargetCursorLedger.record(TARGET, 0, SOURCE, 0, 4L, 20L, 20L, 2, 2);

        assertEquals(first, MVTargetCursorLedger.committed(TARGET, 0, SOURCE, 0));
        assertFalse(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));

        MVTargetCursorLedger.Cursor second = MVTargetCursorLedger.commitCandidatesForTarget(TARGET, 0).get(SOURCE + ".0");
        MVTargetCursorLedger.markCommitted(TARGET, 0, SOURCE, 0, second);

        assertTrue(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));
    }

    public void testIncompleteBatchDoesNotAdvanceCommitCandidate() {
        MVTargetCursorLedger.Cursor floor = new MVTargetCursorLedger.Cursor(2L, 5L);
        MVTargetCursorLedger.seed(TARGET, 0, SOURCE, 0, floor);
        MVTargetCursorLedger.record(TARGET, 0, SOURCE, 0, 3L, 9L, 9L, 4, 3);

        assertEquals(floor, MVTargetCursorLedger.commitCandidatesForTarget(TARGET, 0).get(SOURCE + ".0"));
        assertFalse(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));
    }

    public void testTargetReopenDropsPriorIncarnationAppliedCursor() {
        MVTargetCursorLedger.record(TARGET, 0, SOURCE, 0, 5L, 30L, 30L, 1, 1);
        assertEquals(30L, MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0).checkpoint());

        MVTargetCursorLedger.resetTarget(TARGET, 0);

        assertEquals(MVTargetCursorLedger.Cursor.NONE, MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0));
    }

    public void testPendingBatchIsNotCertifiedUntilPublishingRefreshPromotesIt() {
        MVTargetCursorLedger.stagePending(TARGET, 0, SOURCE, 0, 7L, 41L, 41L, 3, 3);

        assertEquals(MVTargetCursorLedger.Cursor.NONE, MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0));
        assertTrue(MVTargetCursorLedger.commitCandidatesForTarget(TARGET, 0).isEmpty());

        MVTargetCursorLedger.promoteAll(TARGET, 0);

        assertEquals(new MVTargetCursorLedger.Cursor(7L, 41L), MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0));
    }

    public void testPromotionOnlyClaimsBatchesForRefreshedTargetShard() {
        MVTargetCursorLedger.stagePending(TARGET, 0, SOURCE, 0, 3L, 10L, 10L, 1, 1);
        MVTargetCursorLedger.stagePending(TARGET, 1, SOURCE, 1, 4L, 20L, 20L, 1, 1);

        MVTargetCursorLedger.promoteAll(TARGET, 0);

        assertEquals(new MVTargetCursorLedger.Cursor(3L, 10L), MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0));
        assertEquals(MVTargetCursorLedger.Cursor.NONE, MVTargetCursorLedger.certified(TARGET, 1, SOURCE, 1));

        MVTargetCursorLedger.promoteAll(TARGET, 1);
        assertEquals(new MVTargetCursorLedger.Cursor(4L, 20L), MVTargetCursorLedger.certified(TARGET, 1, SOURCE, 1));
    }

    public void testExactChunkClaimsUnionWithoutTrustingFoldCheckpoint() {
        MVTargetCursorLedger.stagePending(
            TARGET,
            0,
            SOURCE,
            0,
            1L,
            20L,
            20L,
            4,
            4,
            MVSourceSeqCoverage.ofSeqNos(java.util.List.of(0L, 2L, 3L, 5L))
        );
        MVTargetCursorLedger.stagePending(
            TARGET,
            0,
            SOURCE,
            0,
            2L,
            20L,
            20L,
            3,
            3,
            MVSourceSeqCoverage.ofSeqNos(java.util.List.of(8L, 10L, 20L))
        );
        MVTargetCursorLedger.stagePending(
            TARGET,
            0,
            SOURCE,
            0,
            3L,
            20L,
            20L,
            4,
            4,
            MVSourceSeqCoverage.ofSeqNos(java.util.List.of(1L, 4L, 6L, 7L))
        );

        MVTargetCursorLedger.promoteAll(TARGET, 0);
        MVSourceSeqCoverage claim = MVTargetCursorLedger.certifiedCoverage(TARGET, 0, SOURCE, 0);

        assertEquals(8L, claim.floor());
        assertEquals(
            java.util.List.of(new MVSourceSeqCoverage.Range(10L, 10L), new MVSourceSeqCoverage.Range(20L, 20L)),
            claim.aboveFloor()
        );
    }

    public void testExactClaimSurvivesCommitMetadataRoundTrip() {
        MVSourceSeqCoverage exact = MVSourceSeqCoverage.ofSeqNos(java.util.List.of(0L, 2L, 3L, 5L, 8L, 10L, 20L));
        MVTargetCursorLedger.Cursor cursor = new MVTargetCursorLedger.Cursor(7L, exact.floor());
        String encoded = MVTargetCursorLedger.encodeCommit(cursor, exact);

        MVTargetCursorLedger.seed(
            TARGET,
            0,
            SOURCE,
            0,
            MVTargetCursorLedger.Cursor.decode(encoded),
            MVTargetCursorLedger.decodeCommitCoverage(encoded)
        );

        assertEquals(cursor, MVTargetCursorLedger.committed(TARGET, 0, SOURCE, 0));
        assertEquals(exact, MVTargetCursorLedger.certifiedCoverage(TARGET, 0, SOURCE, 0));
    }

    public void testSourceCommitCapIsMonotonicAndControlsCommitReadiness() {
        MVTargetCursorLedger.stagePending(
            TARGET,
            0,
            SOURCE,
            0,
            1L,
            0L,
            7L,
            3,
            3,
            MVSourceSeqCoverage.ofSeqNos(java.util.List.of(0L, 2L, 7L))
        );
        MVTargetCursorLedger.promoteAll(TARGET, 0);

        MVTargetCursorLedger.advanceSourceCommitCap(TARGET, 0, SOURCE, 0, 2L);
        MVTargetCursorLedger.advanceSourceCommitCap(TARGET, 0, SOURCE, 0, 1L);
        assertEquals(2L, MVTargetCursorLedger.sourceCommitCap(TARGET, 0, SOURCE, 0));
        assertFalse(MVTargetCursorLedger.allPublishedWithinSourceCommitCaps(TARGET, 0));

        MVTargetCursorLedger.advanceSourceCommitCap(TARGET, 0, SOURCE, 0, 7L);
        assertEquals(7L, MVTargetCursorLedger.sourceCommitCap(TARGET, 0, SOURCE, 0));
        assertTrue(MVTargetCursorLedger.allPublishedWithinSourceCommitCaps(TARGET, 0));
    }

    public void testAboveFloorPublishedBitKeepsMergeClosedUntilExactClaimIsCommitted() {
        MVSourceSeqCoverage durable = MVSourceSeqCoverage.ofSeqNos(java.util.List.of(0L, 1L, 5L));
        MVTargetCursorLedger.seed(TARGET, 0, SOURCE, 0, new MVTargetCursorLedger.Cursor(1L, 1L), durable);
        MVTargetCursorLedger.stagePending(TARGET, 0, SOURCE, 0, 2L, 1L, 7L, 1, 1, MVSourceSeqCoverage.ofSeqNos(java.util.List.of(7L)));
        MVTargetCursorLedger.promoteAll(TARGET, 0);

        assertEquals(1L, MVTargetCursorLedger.certified(TARGET, 0, SOURCE, 0).checkpoint());
        assertFalse(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));

        MVSourceSeqCoverage published = MVTargetCursorLedger.certifiedCoverage(TARGET, 0, SOURCE, 0);
        MVTargetCursorLedger.markCommitted(TARGET, 0, SOURCE, 0, new MVTargetCursorLedger.Cursor(2L, 1L), published);
        assertTrue(MVTargetCursorLedger.allAppliedCommitted(TARGET, 0));
    }
}
