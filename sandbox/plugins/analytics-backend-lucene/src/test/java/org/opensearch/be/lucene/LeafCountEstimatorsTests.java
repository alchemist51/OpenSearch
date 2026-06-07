/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Verifies the per-leaf count estimators backing
 * {@link LuceneFilterDelegationHandle#estimateSelectivityPpm(int, long)}.
 *
 * <p>Index shape: 100 docs, half {@code tag = "hello"}, half {@code tag = "goodbye"}.
 * Lets us check both the {@code TermQuery → docFreq} fast path and the
 * {@code mustNot term → numDocs - docFreq} fallback, plus the per-leaf-min over
 * an all-MUST conjunction.
 */
public class LeafCountEstimatorsTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;
    private LeafReaderContext leaf;
    private IndexSearcher searcher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        writer = new IndexWriter(directory, new IndexWriterConfig());
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("tag", i % 2 == 0 ? "hello" : "goodbye", Field.Store.NO));
            writer.addDocument(doc);
        }
        writer.commit();
        reader = DirectoryReader.open(writer);
        leaf = reader.leaves().get(0);
        searcher = new IndexSearcher(reader);
    }

    @Override
    public void tearDown() throws Exception {
        reader.close();
        writer.close();
        directory.close();
        super.tearDown();
    }

    public void testTermQueryReturnsDocFreq() throws Exception {
        Query q = new TermQuery(new Term("tag", "hello"));
        Weight weight = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafCountEstimator> estimators = LeafCountEstimators.buildList(weight, q);
        assertEquals(1, estimators.size());
        assertEquals("docFreq must equal half the index", 50L, estimators.get(0).estimate(leaf));
    }

    public void testMustNotTermReturnsNumDocsMinusDocFreq() throws Exception {
        BooleanQuery q = new BooleanQuery.Builder().add(new TermQuery(new Term("tag", "hello")), BooleanClause.Occur.MUST_NOT).build();
        Weight weight = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafCountEstimator> estimators = LeafCountEstimators.buildList(weight, q);
        assertEquals(1, estimators.size());
        // Lucene's BooleanWeight.count returns -1 for any query containing MUST_NOT,
        // so we must end up with the dedicated MustNotTermEstimator path here, not the
        // generic Weight.count fallback.
        assertEquals("docs not matching 'hello' = full index minus 50", 50L, estimators.get(0).estimate(leaf));
    }

    public void testMustNotMissingTermReturnsAllDocs() throws Exception {
        BooleanQuery q = new BooleanQuery.Builder().add(new TermQuery(new Term("tag", "missing")), BooleanClause.Occur.MUST_NOT).build();
        Weight weight = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafCountEstimator> estimators = LeafCountEstimators.buildList(weight, q);
        assertEquals("term absent from segment → every doc satisfies !=", 100L, estimators.get(0).estimate(leaf));
    }

    public void testAllMustConjunctionExposesPerLeafEstimators() throws Exception {
        BooleanQuery q = new BooleanQuery.Builder().add(new TermQuery(new Term("tag", "hello")), BooleanClause.Occur.MUST)
            .add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        Weight weight = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafCountEstimator> estimators = LeafCountEstimators.buildList(weight, q);
        // One estimator per MUST clause. Generic clauses (no dedicated leaf path)
        // delegate to the parent BooleanWeight — that's what gives the conjunction-
        // bounded result the caller takes the min over. Both estimators here use the
        // generic path (TermQuery and MatchAllDocsQuery don't trigger the dedicated
        // mustNot-term branch), so each returns the parent's count.
        assertEquals("one estimator per MUST clause", 2, estimators.size());
        long expectedConjunction = searcher.count(q);
        for (LeafCountEstimator estimator : estimators) {
            long c = estimator.estimate(leaf);
            // Either Weight.count had a fast-path (returns the conjunction count) or
            // gave up (-1 → caller consults). Both are acceptable; the rule is no
            // false positives.
            assertTrue("estimate must be -1 or " + expectedConjunction + ", got " + c, c == -1L || c == expectedConjunction);
        }
    }

    public void testNonRecognizedShapeFallsBackToWeightCount() throws Exception {
        // BooleanQuery with a SHOULD clause — not all-MUST, so we fall through to the parent
        // Weight.count. BooleanWeight.count returns -1 for non-leaf queries it doesn't have a
        // fast path for, so the estimator should propagate -1 (caller treats as "consult").
        BooleanQuery q = new BooleanQuery.Builder().add(new TermQuery(new Term("tag", "hello")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term("tag", "goodbye")), BooleanClause.Occur.SHOULD)
            .build();
        Weight weight = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        List<LeafCountEstimator> estimators = LeafCountEstimators.buildList(weight, q);
        assertEquals("non-conjunctive top-level → single fallback estimator", 1, estimators.size());
        long count = estimators.get(0).estimate(leaf);
        // Either the BooleanWeight has a count fast-path (rare) — in which case it equals 100 —
        // or it returns -1. Both are acceptable; the rule is "don't lie".
        assertTrue("count must be -1 (unknown) or full index, got " + count, count == -1L || count == 100L);
    }
}
