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

/**
 * Coverage set-algebra planning tests. The exact complement/intersection
 * semantics verified here drive the pull path's coverage-gap adjustment
 * (rows + noops == range) in {@code MVDerivedArtifactBuilder}.
 */
public class MVRecoveryPlanningTests extends OpenSearchTestCase {

    public void testMissingThroughProducesExactComplementRanges() {
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
    }

    public void testDurablyKnownNoOpsAreSubtractedFromDataCoverage() {
        MVSourceSeqCoverage missing = MVSourceSeqCoverage.ofRanges(List.of(new MVSourceSeqCoverage.Range(4L, 8L)));
        MVSourceSeqCoverage knownNoOps = MVSourceSeqCoverage.ofSeqNos(List.of(5L, 6L, 8L, 10L));

        MVSourceSeqCoverage provenNoOps = missing.intersection(knownNoOps);
        MVSourceSeqCoverage dataCoverage = missing.subtract(provenNoOps);

        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(5L, 6L, 8L)), provenNoOps);
        assertEquals(MVSourceSeqCoverage.ofSeqNos(List.of(4L, 7L)), dataCoverage);

        MVSourceSeqCoverage noOpOnly = missing.subtract(missing.intersection(missing));
        assertEquals(MVSourceSeqCoverage.EMPTY, noOpOnly);
    }
}
