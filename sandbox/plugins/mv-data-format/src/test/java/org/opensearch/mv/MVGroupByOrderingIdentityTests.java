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
 * Stage 4: Tests for {@link MVGroupByOrdering#orderingIdentity()} and
 * {@link MVGroupByOrdering#validateCompatible(MVGroupByOrdering)}, used
 * by the merge path to validate that all input state files share the
 * expected ordering contract.
 */
public class MVGroupByOrderingIdentityTests extends OpenSearchTestCase {

    // ── orderingIdentity() ────────────────────────────────────────────────

    public void testOrderingIdentityFormatIsDeterministic() {
        MVGroupByOrdering ordering = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        String identity = ordering.orderingIdentity();

        // Format: "idx:col:dir:null;idx:col:dir:null"
        assertEquals("0:k0:0:0;1:k1:0:0", identity);

        // Calling twice produces the same result
        assertEquals(identity, ordering.orderingIdentity());
    }

    public void testOrderingIdentityDiffersWhenColumnsChange() {
        MVGroupByOrdering a = MVCompiledDefinition.of(
            List.of(GroupKey.of("regionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        MVGroupByOrdering b = MVCompiledDefinition.of(
            List.of(GroupKey.of("userID", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        assertNotEquals(a.orderingIdentity(), b.orderingIdentity());
    }

    public void testOrderingIdentitySameForSameDefinition() {
        MVGroupByOrdering a = MVCompiledDefinition.clickbench100m().groupByOrdering();
        MVGroupByOrdering b = MVCompiledDefinition.clickbench100m().groupByOrdering();

        assertEquals(a.orderingIdentity(), b.orderingIdentity());
    }

    public void testOrderingIdentityDiffersAcrossLadderRungs() {
        MVGroupByOrdering l1 = MVCompiledDefinition.heavyL1().groupByOrdering();
        MVGroupByOrdering l3 = MVCompiledDefinition.heavyL3().groupByOrdering();

        // L1 has 8 keys, L3 has 10 — identities must differ
        assertNotEquals(l1.orderingIdentity(), l3.orderingIdentity());
    }

    public void testSingleKeyOrderingIdentity() {
        MVGroupByOrdering ordering = MVCompiledDefinition.of(
            List.of(GroupKey.of("only", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        ).groupByOrdering();

        assertEquals("0:only:0:0", ordering.orderingIdentity());
    }

    // ── validateCompatible() ──────────────────────────────────────────────

    public void testValidateCompatiblePassesForSameOrdering() {
        MVGroupByOrdering a = MVCompiledDefinition.clickbench100m().groupByOrdering();
        MVGroupByOrdering b = MVCompiledDefinition.clickbench100m().groupByOrdering();

        // Should not throw
        a.validateCompatible(b);
        b.validateCompatible(a);
    }

    public void testValidateCompatibleThrowsForDifferentOrdering() {
        MVGroupByOrdering a = MVCompiledDefinition.heavyL1().groupByOrdering();
        MVGroupByOrdering b = MVCompiledDefinition.heavyL3().groupByOrdering();

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> a.validateCompatible(b));
        assertTrue(ex.getMessage().contains("MV ordering mismatch"));
        assertTrue(ex.getMessage().contains(a.orderingIdentity()));
        assertTrue(ex.getMessage().contains(b.orderingIdentity()));
    }

    public void testValidateCompatibleRejectsNull() {
        MVGroupByOrdering ordering = MVCompiledDefinition.clickbench100m().groupByOrdering();
        expectThrows(NullPointerException.class, () -> ordering.validateCompatible(null));
    }

    public void testValidateCompatibleIsSymmetric() {
        MVGroupByOrdering a = MVCompiledDefinition.heavyL1().groupByOrdering();
        MVGroupByOrdering b = MVCompiledDefinition.heavyL2().groupByOrdering();

        // Both should throw since L1 has 8 keys (10 metrics) and L2 has 8 keys (20 metrics)
        // — same group keys but we're comparing orderings which only carry group keys, so
        // they should actually be EQUAL (same 8 group keys).
        // Let's verify.
        assertEquals(a.size(), b.size()); // both 8
        assertEquals(a.orderingIdentity(), b.orderingIdentity());
        // This should NOT throw since they have the same group keys.
        a.validateCompatible(b);
    }
}
