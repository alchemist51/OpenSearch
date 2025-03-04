///*
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// */
//
//package org.opensearch.search.internal;
//
//import org.apache.lucene.search.BulkScorer;
//import org.apache.lucene.search.DocIdSetIterator;
//import org.apache.lucene.search.LeafCollector;
//import org.apache.lucene.search.Scorer;
//import org.apache.lucene.search.TwoPhaseIterator;
//import org.apache.lucene.util.BitSet;
//import org.apache.lucene.util.Bits;
//
//import java.io.IOException;
//
//public class CustomParquetBulkScorer extends BulkScorer {
//    private final Scorer scorer;
//    private final DocIdSetIterator iterator;
//    private final TwoPhaseIterator twoPhase;
//
//    public CustomParquetBulkScorer(Scorer scorer) {
//        if (scorer == null) {
//            throw new NullPointerException();
//        } else {
//            this.scorer = scorer;
//            this.iterator = scorer.iterator();
//            this.twoPhase = scorer.twoPhaseIterator();
//        }
//    }
//
//    @Override
//    public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
//        collector.setScorer(scorer);
//        DocIdSetIterator scorerIterator = this.twoPhase == null ? this.iterator : this.twoPhase.approximation();
//        ParquetBitSet bitset = new ParquetBitSet(max);
//        for(int doc = iterator.nextDoc(); doc != Integer.MAX_VALUE; doc = iterator.nextDoc()) {
//            bitset.set(doc);
//        }
//
//        // Set context of the query here in the FilterExec and from the results run the collectBatch for leafCollector
//        // Create a FilterExec and pass it into the ParquetExec and collect the result
//        DocIdSetIterator competitiveIterator = collector.competitiveIterator();
//        if (competitiveIterator == null && scorerIterator.docID() == -1 && min == 0 && max == Integer.MAX_VALUE) {
//                if (twoPhase == null) {
//                    for(int doc = iterator.nextDoc(); doc != Integer.MAX_VALUE; doc = iterator.nextDoc()) {
//                        if (acceptDocs == null || acceptDocs.get(doc)) {
//                            collector.collect(doc);
//                        }
//                    }
//                } else {
//                    for(int doc = iterator.nextDoc(); doc != Integer.MAX_VALUE; doc = iterator.nextDoc()) {
//                        if ((acceptDocs == null || acceptDocs.get(doc)) && twoPhase.matches()) {
//                            collector.collect(doc);
//                        }
//                    }
//                }
//
//            return Integer.MAX_VALUE;
//        } else {
//            throw new NullPointerException();
//        }
//    }
//
//
//    @Override
//    public long cost() {
//        return iterator.cost();
//    }
//}
//
//private final Scorer scorer;
//private final DocIdSetIterator iterator;
//private final TwoPhaseIterator twoPhase;
//
//public DefaultBulkScorer(Scorer scorer) {
//    if (scorer == null) {
//        throw new NullPointerException();
//    } else {
//        this.scorer = scorer;
//        this.iterator = scorer.iterator();
//        this.twoPhase = scorer.twoPhaseIterator();
//    }
//}
//
//public long cost() {
//    return this.iterator.cost();
//}
//
//public int score(LeafCollector collector, Bits acceptDocs, int min, int max) throws IOException {
//    collector.setScorer(this.scorer);
//    DocIdSetIterator scorerIterator = this.twoPhase == null ? this.iterator : this.twoPhase.approximation();
//    DocIdSetIterator competitiveIterator = collector.competitiveIterator();
//    if (competitiveIterator == null && scorerIterator.docID() == -1 && min == 0 && max == Integer.MAX_VALUE) {
//        scoreAll(collector, scorerIterator, this.twoPhase, acceptDocs);
//        return Integer.MAX_VALUE;
//    } else {
//        return scoreRange(collector, scorerIterator, this.twoPhase, competitiveIterator, acceptDocs, min, max);
//    }
//}
