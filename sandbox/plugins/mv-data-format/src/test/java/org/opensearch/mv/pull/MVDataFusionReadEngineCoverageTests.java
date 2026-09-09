/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.mv.pull;

import org.opensearch.test.OpenSearchTestCase;

/** Tests definition-driven coverage reduction across DataFusion partial rows. */
public class MVDataFusionReadEngineCoverageTests extends OpenSearchTestCase {

    public void testCombinesEveryPartialCoverageRow() {
        Long[] counts = { 11_139L, 321_861L, 342_000L };
        Long[] maxima = { 11_138L, 332_999L, 674_999L };
        MVDataFusionReadEngine.CoverageTotals coverage = MVDataFusionReadEngine.reduceCoverageRows(
            counts.length,
            row -> counts[row],
            row -> maxima[row]
        );
        assertEquals(675_000L, coverage.totalRows());
        assertEquals(674_999L, coverage.observedMaxSeqNo());
    }

    public void testEmptyCoverageHasNoObservedSequenceNumber() {
        MVDataFusionReadEngine.CoverageTotals coverage = MVDataFusionReadEngine.reduceCoverageRows(0, row -> {
            fail("count accessor must not be called");
            return null;
        }, row -> {
            fail("max accessor must not be called");
            return null;
        });
        assertEquals(0L, coverage.totalRows());
        assertEquals(-1L, coverage.observedMaxSeqNo());
    }

    public void testNullPartialValuesAreIgnored() {
        Long[] counts = { 25L, null };
        Long[] maxima = { null, 24L };
        MVDataFusionReadEngine.CoverageTotals coverage = MVDataFusionReadEngine.reduceCoverageRows(
            counts.length,
            row -> counts[row],
            row -> maxima[row]
        );
        assertEquals(25L, coverage.totalRows());
        assertEquals(24L, coverage.observedMaxSeqNo());
    }

    public void testCountOverflowFailsClosed() {
        Long[] counts = { Long.MAX_VALUE, 1L };
        Long[] maxima = { 0L, 1L };
        expectThrows(
            ArithmeticException.class,
            () -> MVDataFusionReadEngine.reduceCoverageRows(counts.length, row -> counts[row], row -> maxima[row])
        );
    }
}
