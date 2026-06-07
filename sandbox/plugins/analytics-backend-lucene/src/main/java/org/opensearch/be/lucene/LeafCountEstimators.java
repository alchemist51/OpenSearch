/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.List;

/**
 * Static factory + concrete {@link LeafCountEstimator} implementations.
 *
 * <p>{@link #buildList(Weight, Query)} decomposes a compiled query into a list of
 * per-leaf estimators — one per AND-clause when the top-level shape is a
 * {@link BooleanQuery} of MUST clauses, or a single estimator for an atomic shape.
 * The driver evaluates every estimator on a leaf and takes the minimum, since a
 * conjunction's count is bounded by the smallest leaf's count.
 */
final class LeafCountEstimators {

    private LeafCountEstimators() {}

    /**
     * Build the per-leaf estimators for one delegated provider's compiled query.
     * Returns a singleton list for non-conjunctive shapes; one entry per MUST
     * clause for top-level {@code BooleanQuery} of all-MUST clauses.
     *
     * <p>Each returned estimator is independent — the runtime evaluates them on a
     * leaf and takes {@code min}; any {@code -1} poisons the result to {@code -1}.
     */
    static List<LeafCountEstimator> buildList(Weight weight, Query rewrittenQuery) {
        if (rewrittenQuery instanceof BooleanQuery bq && isAllMust(bq)) {
            // Each MUST clause becomes its own estimator. The compiled Weight covers the
            // full bool query, so for the Weight.count fast-path we still pass the parent
            // weight for clauses we don't recognize individually — Lucene's BooleanWeight
            // happens to short-circuit -1 anyway, so they degenerate to "unknown" which is
            // the safe answer. Recognized leaf shapes (TermQuery, mustNot-term BoolQuery)
            // get their own dedicated estimators that bypass BooleanWeight.count.
            java.util.ArrayList<LeafCountEstimator> result = new java.util.ArrayList<>(bq.clauses().size());
            for (BooleanClause clause : bq.clauses()) {
                result.add(forSingleQuery(clause.query(), weight));
            }
            return result;
        }
        return List.of(forSingleQuery(rewrittenQuery, weight));
    }

    private static boolean isAllMust(BooleanQuery bq) {
        for (BooleanClause clause : bq.clauses()) {
            if (clause.occur() != BooleanClause.Occur.MUST) {
                return false;
            }
        }
        return true;
    }

    /**
     * Pick the right estimator for one (possibly nested) query shape. Falls back to
     * the parent {@link Weight}'s {@code count} when the shape doesn't match a
     * dedicated path — which itself returns {@code -1} for any
     * {@link BooleanQuery} carrying a {@code MUST_NOT} clause, so the driver
     * defaults to consulting in that case.
     */
    private static LeafCountEstimator forSingleQuery(Query q, Weight parentWeight) {
        if (q instanceof TermQuery tq) {
            return new WeightCountEstimator(parentWeight);
        }
        Term mustNotTerm = extractMustNotTerm(q);
        if (mustNotTerm != null) {
            return new MustNotTermEstimator(mustNotTerm);
        }
        return new WeightCountEstimator(parentWeight);
    }

    /**
     * If {@code q} has shape {@code BooleanQuery { mustNot: TermQuery }} (with
     * exactly that one clause), returns the inner term. Otherwise {@code null}.
     * The {@link org.opensearch.be.lucene.serializers.NotEqualsSerializer} emits
     * exactly this shape for {@code col != literal} on keyword fields — keeping
     * the matcher narrow keeps a future serializer change from accidentally
     * activating this path on a different shape that happens to contain
     * {@code MUST_NOT}.
     */
    private static Term extractMustNotTerm(Query q) {
        if (q instanceof BooleanQuery bq && bq.clauses().size() == 1) {
            BooleanClause clause = bq.clauses().get(0);
            if (clause.occur() == BooleanClause.Occur.MUST_NOT && clause.query() instanceof TermQuery tq) {
                return tq.getTerm();
            }
        }
        return null;
    }

    /**
     * Delegates to {@link Weight#count(LeafReaderContext)} which Lucene implements
     * natively for atomic shapes (TermQuery → docFreq, PointRangeQuery → BKD count,
     * MatchAllDocsQuery → numDocs). Returns {@code -1} when Lucene doesn't have a
     * fast-path — most notably for any {@link BooleanQuery} containing a
     * {@code MUST_NOT}, which is why the {@link MustNotTermEstimator} below
     * exists.
     */
    static final class WeightCountEstimator implements LeafCountEstimator {
        private final Weight weight;

        WeightCountEstimator(Weight weight) {
            this.weight = weight;
        }

        @Override
        public long estimate(LeafReaderContext leaf) throws IOException {
            int count = weight.count(leaf);
            // Weight.count returns -1 when Lucene has no fast-path (e.g. BooleanWeight
            // with a MUST_NOT clause); preserve that so the driver consults. Otherwise
            // widen to long (counts are non-negative when known).
            return count < 0 ? -1L : count;
        }
    }

    /**
     * Counts {@code numDocs - docFreq(term)} on the leaf — exact for
     * {@code mustNot term} on a non-tombstoned term, conservative when the term is
     * absent (returns the full segment doc count, which is correct: every doc
     * matches "not equal to a value that doesn't exist").
     */
    static final class MustNotTermEstimator implements LeafCountEstimator {
        private final Term term;

        MustNotTermEstimator(Term term) {
            this.term = term;
        }

        @Override
        public long estimate(LeafReaderContext leaf) throws IOException {
            int total = leaf.reader().numDocs();
            if (total <= 0) {
                return 0;
            }
            Terms terms = leaf.reader().terms(term.field());
            if (terms == null) {
                // Field absent on this segment — every doc satisfies `mustNot term(absent)`.
                return total;
            }
            TermsEnum it = terms.iterator();
            if (!it.seekExact(term.bytes())) {
                return total;
            }
            return Math.max(0L, (long) total - (long) it.docFreq());
        }
    }
}
