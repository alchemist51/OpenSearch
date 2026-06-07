/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;

/**
 * {@link PerfPeerNarrowingRule} that drops Lucene as a performance-delegation peer
 * for predicates of shape {@code col != ''} when the driver is DataFusion.
 *
 * <p><b>Why.</b> A {@code !=}-against-empty-string predicate matches almost every
 * document in a typical index (only docs whose value is exactly the empty string
 * fail the test). Asking Lucene for a bitset of matching docs and AND-intersecting
 * it into the candidate set buys nothing: the bitset is approximately the entire
 * doc-id range, so no candidates get pruned. The cost of the FFM round-trip per
 * row group is paid for no benefit. Worse, on text fields the analyzer produces
 * zero tokens from the empty string, so {@code mustNot term {field: ""}}
 * degenerates to "match all" — strictly wrong filtering — but the keyword-only
 * {@link org.opensearch.analytics.spi.FilterCapability} restriction blocks that
 * shape from reaching the runtime.
 *
 * <p>Lucene-as-driver alternatives are unaffected: this rule only fires when the
 * stage's resolved driver is DataFusion and Lucene appears as a perf peer.
 * Stage alternatives where Lucene is the chosen driver (e.g. {@code count(*)
 * WHERE col != ''} fast path via {@code IndexSearcher.count()}) are scored
 * independently by {@code LuceneShardPreference} and still win where they
 * should — this pass doesn't see them as having a "peer" relationship.
 *
 * @opensearch.internal
 */
final class DropLucenePeerForNotEqualsEmptyStringRule implements PerfPeerNarrowingRule {

    private static final String NAME = "drop-lucene-perf-peer-for-not-equals-empty-string";

    private final String driverBackend;
    private final String peerBackend;

    /** Production wiring: driver = "datafusion", peer = "lucene". */
    DropLucenePeerForNotEqualsEmptyStringRule() {
        this("datafusion", "lucene");
    }

    /** Test wiring: caller passes mock backend names ("mock-parquet", "mock-lucene"). */
    DropLucenePeerForNotEqualsEmptyStringRule(String driverBackend, String peerBackend) {
        this.driverBackend = driverBackend;
        this.peerBackend = peerBackend;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean shouldDropPeer(String driverBackend, String peerBackend, AnnotatedPredicate predicate, RexCall original) {
        if (!this.driverBackend.equals(driverBackend) || !this.peerBackend.equals(peerBackend)) {
            return false;
        }
        if (original.getKind() != SqlKind.NOT_EQUALS) {
            return false;
        }
        // Either operand may be the literal — `col != ''` and `'' != col` are both valid Calcite
        // shapes. The predicate's other operand is typically a RexInputRef but we don't care here;
        // any presence of an empty-string literal under NOT_EQUALS is enough for the rule.
        for (RexNode operand : original.getOperands()) {
            if (operand instanceof RexLiteral lit && isEmptyString(lit)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isEmptyString(RexLiteral lit) {
        Object value = lit.getValue();
        if (value instanceof NlsString nls) {
            return nls.getValue().isEmpty();
        }
        if (value instanceof String s) {
            return s.isEmpty();
        }
        return false;
    }
}
