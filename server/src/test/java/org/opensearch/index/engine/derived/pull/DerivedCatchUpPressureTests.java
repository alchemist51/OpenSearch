/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Defect #26 registry semantics: counted node-wide claims, defensive floor,
 * release listeners fire exactly on the last-claim release.
 */
public class DerivedCatchUpPressureTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        DerivedCatchUpPressure.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        DerivedCatchUpPressure.clearForTests();
        super.tearDown();
    }

    public void testInactiveByDefault() {
        assertFalse(DerivedCatchUpPressure.isActive());
    }

    public void testClaimActivatesReleaseDeactivates() {
        DerivedCatchUpPressure.claim();
        assertTrue(DerivedCatchUpPressure.isActive());
        DerivedCatchUpPressure.release();
        assertFalse(DerivedCatchUpPressure.isActive());
    }

    public void testCountedClaimsAcrossPollers() {
        DerivedCatchUpPressure.claim(); // poller A
        DerivedCatchUpPressure.claim(); // poller B
        DerivedCatchUpPressure.release(); // A caught up — B still catching up
        assertTrue("pressure must remain while any poller is catching up", DerivedCatchUpPressure.isActive());
        DerivedCatchUpPressure.release();
        assertFalse(DerivedCatchUpPressure.isActive());
    }

    public void testUnmatchedReleaseDoesNotPoisonFutureClaims() {
        DerivedCatchUpPressure.release(); // unmatched
        assertFalse(DerivedCatchUpPressure.isActive());
        DerivedCatchUpPressure.claim();
        assertTrue("floor at zero: a later claim must still activate", DerivedCatchUpPressure.isActive());
        DerivedCatchUpPressure.release();
        assertFalse(DerivedCatchUpPressure.isActive());
    }

    public void testListenerFiresExactlyOnLastRelease() {
        AtomicInteger fired = new AtomicInteger();
        Runnable listener = fired::incrementAndGet;
        DerivedCatchUpPressure.addOnReleaseListener(listener);

        DerivedCatchUpPressure.claim();
        DerivedCatchUpPressure.claim();
        DerivedCatchUpPressure.release();
        assertEquals("no re-admission while another poller still catches up", 0, fired.get());
        DerivedCatchUpPressure.release();
        assertEquals("re-admission exactly at the last release", 1, fired.get());

        DerivedCatchUpPressure.removeOnReleaseListener(listener);
        DerivedCatchUpPressure.claim();
        DerivedCatchUpPressure.release();
        assertEquals("removed listener must not fire", 1, fired.get());
    }

    public void testListenerExceptionDoesNotBreakOtherListeners() {
        AtomicInteger fired = new AtomicInteger();
        Runnable failing = () -> { throw new RuntimeException("listener failure"); };
        Runnable counting = fired::incrementAndGet;
        DerivedCatchUpPressure.addOnReleaseListener(failing);
        DerivedCatchUpPressure.addOnReleaseListener(counting);

        DerivedCatchUpPressure.claim();
        DerivedCatchUpPressure.release();
        assertEquals("a failing listener must not suppress the rest", 1, fired.get());
    }
}
