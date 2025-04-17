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
        int i=0;
        for(LeafReaderContext ctx : sortedLeaves) {
            // We have numberOfSlices, let's see, we will need rowGroups in the leaf.
            int rowGroups = 0;
            if(i==0) {
                rowGroups = 9;
            } else {
                rowGroups = 6;
            }

            int avg_row_group_per_partition = rowGroups / numberOfSlices;
            int start_doc = 0;
            int end_doc = 0;
            for(int j=0; j<numberOfSlices - 1; j++) {
                start_doc = end_doc;
                end_doc = start_doc + avg_row_group_per_partition * 1024 * 1024;
                //groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, start_doc, end_doc)));
                groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, 0, ctx.reader().maxDoc())));
            }

            start_doc = end_doc;
            end_doc = ctx.reader().maxDoc();
            groupedLeafPartitions.add(Collections.singletonList(IndexSearcher.LeafReaderContextPartition.createFromAndTo(ctx, start_doc, end_doc)));
            i++;
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
