/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import java.util.Objects;

/**
 * The planner's record that a query's aggregate-over-scan subtree is servable
 * from a materialized view's state files, subject to shard-local coverage.
 *
 * <p>Deliberately <b>not</b> attached to the RelNode tree (decision D1 in the
 * search-side integration plan): marking, CBO, and the split rules copy and
 * rebuild rels, so tree-attached metadata can be silently dropped. Instead the
 * annotation lives in {@code PlannerContext}, keyed by table identity, and is
 * consumed once when the shard fragment is emitted.
 *
 * <p>The annotation is an <i>option</i>, not a rewrite: the plan is left
 * untouched, and the shard decides per segment (coverage from its catalog
 * snapshot) whether to exercise it. A shard that drops the binding (schema
 * fingerprint mismatch, zero coverage) runs today's raw plan — never wrong,
 * only slower.
 *
 * @param mvId                   the matched MV
 * @param stateSchemaFingerprint expected state-file schema; the shard verifies
 *                               this against its snapshot before binding
 *
 * @opensearch.internal
 */
public record MVRewriteAnnotation(String mvId, String stateSchemaFingerprint) {
    public MVRewriteAnnotation {
        Objects.requireNonNull(mvId, "mvId");
        Objects.requireNonNull(stateSchemaFingerprint, "stateSchemaFingerprint");
    }
}
