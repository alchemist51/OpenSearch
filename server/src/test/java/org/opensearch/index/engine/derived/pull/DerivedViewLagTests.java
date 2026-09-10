/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

public class DerivedViewLagTests extends OpenSearchTestCase {

    private static final long S = TimeUnit.SECONDS.toNanos(1);

    @Before
    @After
    public void clearRegistry() {
        DerivedViewLag.clearForTests();
    }

    public void testRegistryMinOverViewsAndRemoval() {
        assertEquals(Long.MAX_VALUE, DerivedViewLag.minWatermark("src", 0));
        assertNull(DerivedViewLag.slowestView("src", 0));
        DerivedViewLag.record("src", 0, "src_mv_a", 100);
        DerivedViewLag.record("src", 0, "src_mv_b", 40);
        DerivedViewLag.record("src", 1, "src_mv_a", 5); // another shard: independent
        assertEquals(40, DerivedViewLag.minWatermark("src", 0));
        assertEquals("src_mv_b", DerivedViewLag.slowestView("src", 0));
        assertEquals(5, DerivedViewLag.minWatermark("src", 1));
        // a watermark never moves backwards (re-publication of an older generation)
        DerivedViewLag.record("src", 0, "src_mv_b", 30);
        assertEquals(40, DerivedViewLag.minWatermark("src", 0));
        DerivedViewLag.record("src", 0, "src_mv_b", 150);
        assertEquals(100, DerivedViewLag.minWatermark("src", 0));
        assertEquals("src_mv_a", DerivedViewLag.slowestView("src", 0));
        DerivedViewLag.remove("src", 0, "src_mv_a");
        assertEquals(150, DerivedViewLag.minWatermark("src", 0));
        DerivedViewLag.remove("src", 0, "src_mv_b");
        assertEquals(Long.MAX_VALUE, DerivedViewLag.minWatermark("src", 0));
    }

    public void testStalenessIsAgeOfOldestMissingDocument() {
        DerivedViewLag.Tracker t = new DerivedViewLag.Tracker(0, 1000);
        // shard indexes 100 docs per second for 10 s
        for (int sec = 1; sec <= 10; sec++) {
            t.sample(sec * S, sec * 100L - 1);
        }
        long now = 10 * S;
        // view complete: nothing missing
        assertEquals(0.0, t.staleness(now, 999, 999), 1e-9);
        assertEquals(0.0, t.staleness(now, 5000, 999), 1e-9);
        // view at seqNo 499: oldest missing doc is 500, indexed within the sample taken at t=6 s → 4 s old
        assertEquals(4.0, t.staleness(now, 499, 999), 1e-9);
        // view at seqNo 99: oldest missing doc 100 → sample at t=2 s → 8 s old
        assertEquals(8.0, t.staleness(now, 99, 999), 1e-9);
        // the missing document is newer than the last sample: not yet attributable, 0
        assertEquals(0.0, t.staleness(now, 999, 1200), 1e-9);
    }

    public void testStalenessOlderThanTheRingIsBoundedBySpan() {
        DerivedViewLag.Tracker t = new DerivedViewLag.Tracker(0, 3);
        t.sample(1 * S, 100);
        t.sample(2 * S, 200);
        t.sample(3 * S, 300);
        t.sample(4 * S, 400); // evicts the t=1 s sample
        assertEquals(3, t.size());
        // view at seqNo 50: the missing doc 51 was indexed before the oldest sample (t=2 s, seqNo 200) → at least 8 s old at t=10 s
        assertEquals(8.0, t.staleness(10 * S, 50, 400), 1e-9);
    }

    public void testSamplesAreSpacedAndMonotone() {
        DerivedViewLag.Tracker t = new DerivedViewLag.Tracker(S, 100);
        t.sample(0, 10);
        t.sample(S / 2, 20); // too soon: dropped
        t.sample(S, 20); // spacing ok and progress since the last kept sample: recorded
        t.sample(3 * S / 2, 20); // no progress: dropped
        t.sample(2 * S, 30);
        assertEquals(3, t.size());
        // seqNo 15 (missing when the view is at 14) was indexed between the t=0 and t=1 s samples → attributed to t=1 s → 4 s old at t=5 s
        assertEquals(4.0, t.staleness(5 * S, 14, 30), 1e-9);
    }
}
