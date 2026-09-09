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
 * Tests for the physical ordering identity derivation that fixes the
 * compaction ordering-identity mismatch between Java (which uses SQL aliases)
 * and Rust (which uses DataFusion's Partial-aggregate physical column names
 * from the Parquet state-file schema).
 */
public class MVGroupByOrderingIdentityTests extends OpenSearchTestCase {

    /**
     * Verify that physicalOrderingIdentity substitutes the physical column
     * names into the identity string, replacing logical aliases.
     *
     * This is the core invariant: when an expression group key like
     * {@code floor(EventTime/300000) AS event_bucket} produces a Partial
     * aggregate output column named {@code mv_input.EventTime / Int64(300000)},
     * the merge params must use that physical name — NOT the alias
     * {@code event_bucket} — so the Rust merge_state_streams identity
     * comparison succeeds.
     */
    public void testPhysicalOrderingIdentitySubstitutesExpressionKeyNames() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG, "\"EventTime\" / 300000", "EventTime"),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();

        // The LOGICAL identity uses the alias:
        assertEquals("0:event_bucket:0:0;1:URL:0:0;2:UserID:0:0", ordering.orderingIdentity());

        // The PHYSICAL identity substitutes the physical name from the file:
        List<String> physicalNames = List.of(
            "mv_input.EventTime / Int64(300000)", // DataFusion's Partial output
            "URL",       // plain column — same as alias
            "UserID"     // plain column — same as alias
        );
        assertEquals(
            "0:mv_input.EventTime / Int64(300000):0:0;1:URL:0:0;2:UserID:0:0",
            ordering.physicalOrderingIdentity(physicalNames)
        );
    }

    /**
     * physicalOrderingIdentity with plain-only group keys produces the same
     * result as the logical orderingIdentity (no substitution needed).
     */
    public void testPhysicalOrderingIdentityMatchesLogicalForPlainKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("region", GroupKey.ColumnType.LONG), GroupKey.of("os", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        String logical = ordering.orderingIdentity();
        String physical = ordering.physicalOrderingIdentity(List.of("region", "os"));
        assertEquals(logical, physical);
    }

    /**
     * physicalOrderingIdentity rejects mismatched list sizes.
     */
    public void testPhysicalOrderingIdentityRejectsSizeMismatch() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        expectThrows(IllegalArgumentException.class, () -> ordering.physicalOrderingIdentity(List.of("a", "b")));
    }
}
