/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class MVRecoveryPlanningTests extends OpenSearchTestCase {

    public void testPredicateContainsOnlyExactComplementRanges() {
        MVSourceSeqCoverage targetClaim = MVSourceSeqCoverage.ofSeqNos(List.of(0L, 2L, 3L, 5L, 8L, 10L));
        List<MVSourceSeqCoverage.Range> missing = targetClaim.missingThrough(10L);

        assertEquals(
            List.of(
                new MVSourceSeqCoverage.Range(1L, 1L),
                new MVSourceSeqCoverage.Range(4L, 4L),
                new MVSourceSeqCoverage.Range(6L, 7L),
                new MVSourceSeqCoverage.Range(9L, 9L)
            ),
            missing
        );
        assertEquals(
            "(\"_seq_no\" >= 1 AND \"_seq_no\" <= 1) OR "
                + "(\"_seq_no\" >= 4 AND \"_seq_no\" <= 4) OR "
                + "(\"_seq_no\" >= 6 AND \"_seq_no\" <= 7) OR "
                + "(\"_seq_no\" >= 9 AND \"_seq_no\" <= 9)",
            MVIndexingEngine.recoveryPredicate(missing)
        );
    }

    public void testUnexplainedMissingOperationWithNoSourceRowsFailsLoudly() {
        MVSourceSeqCoverage missing = MVSourceSeqCoverage.ofRanges(List.of(new MVSourceSeqCoverage.Range(4L, 6L)));

        IllegalStateException failure = expectThrows(IllegalStateException.class, () -> MVIndexingEngine.requireRecoveryRows(missing, 0L));
        assertTrue(failure.getMessage(), failure.getMessage().contains("unexplained source-data hole"));
        assertTrue(failure.getMessage(), failure.getMessage().contains(missing.encode()));

        MVIndexingEngine.requireRecoveryRows(missing, 1L);
    }

    public void testDurablyKnownNoOpsAreRemovedFromRecoveryDataPredicate() {
        MVSourceSeqCoverage missing = MVSourceSeqCoverage.ofRanges(List.of(new MVSourceSeqCoverage.Range(4L, 8L)));
        MVSourceSeqCoverage knownNoOps = MVSourceSeqCoverage.ofSeqNos(List.of(5L, 6L, 8L, 10L));

        MVSourceSeqCoverage provenNoOps = missing.intersection(knownNoOps);
        MVSourceSeqCoverage dataCoverage = missing.subtract(provenNoOps);

        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(5L, 6L, 8L)), provenNoOps);
        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(4L, 7L)), dataCoverage);
        assertEquals(
            "(\"_seq_no\" >= 4 AND \"_seq_no\" <= 4) OR (\"_seq_no\" >= 7 AND \"_seq_no\" <= 7)",
            MVIndexingEngine.recoveryPredicate(dataCoverage.ranges())
        );

        MVSourceSeqCoverage noOpOnly = missing.subtract(missing.intersection(missing));
        assertEquals(MVSourceSeqCoverage.EMPTY, noOpOnly);
        MVIndexingEngine.requireRecoveryRows(noOpOnly, 0L);
    }
}
