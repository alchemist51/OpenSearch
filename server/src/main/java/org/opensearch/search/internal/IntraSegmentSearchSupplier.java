/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class IntraSegmentSearchSupplier {

    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int numberOfSlices) {
        List<LeafReaderContext> sortedLeaves = new ArrayList(leaves);
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt((l) -> l.reader().maxDoc())));

        List<List<IndexSearcher.LeafReaderContextPartition>> groupedLeafPartitions = new ArrayList();

        for(LeafReaderContext ctx : sortedLeaves) {
            int numSlices = numberOfSlices;
            int numDocs = ctx.reader().maxDoc() / numSlices;
            int maxDocId = numDocs;
            int minDocId = 0;

            for(int i = 0; i < numSlices - 1; ++i) {
//                groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, minDocId, maxDocId)));
                groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, 0, ctx.reader().maxDoc())));
                minDocId = maxDocId;
                maxDocId += numDocs;
            }

//            groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, minDocId, ctx.reader().maxDoc())));
            groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, 0, ctx.reader().maxDoc())));

        }


        IndexSearcher.LeafSlice[] slices = new IndexSearcher.LeafSlice[groupedLeafPartitions.size()];
        int upto = 0;

        for(List<IndexSearcher.LeafReaderContextPartition> currentGroup : groupedLeafPartitions) {
            slices[upto] = new IndexSearcher.LeafSlice(currentGroup);
            ++upto;
        }

        return slices;
    }
}
