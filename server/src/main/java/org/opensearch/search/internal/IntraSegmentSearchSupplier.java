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

    static IndexSearcher.LeafSlice[] getSlices(List<LeafReaderContext> leaves, int targetMaxSlice) {

        if (targetMaxSlice <= 0) {
            throw new IllegalArgumentException("MaxTargetSliceSupplier called with unexpected slice count of " + targetMaxSlice);
        }
        // slice count should not exceed the segment count
        int targetSliceCount = Math.min(targetMaxSlice, leaves.size());

        // Make a copy so we can sort:
        List<LeafReaderContext> sortedLeaves = new ArrayList<>(leaves);

        // Sort by maxDoc, descending:
        sortedLeaves.sort(Collections.reverseOrder(Comparator.comparingInt(l -> l.reader().maxDoc())));

        final List<List<IndexSearcher.LeafReaderContextPartition>> groupedLeaves = new ArrayList<>(targetSliceCount);
        for (int i = 0; i < targetSliceCount; ++i) {
            groupedLeaves.add(new ArrayList<>());
        }
        // distribute the slices in round-robin fashion
        for (int idx = 0; idx < sortedLeaves.size(); ++idx) {
            int currentGroup = idx % targetSliceCount;
            groupedLeaves.get(currentGroup).add(IndexSearcher.LeafReaderContextPartition.createForEntireSegment(sortedLeaves.get(idx)));
        }

        return groupedLeaves.stream().map(IndexSearcher.LeafSlice::new).toArray(IndexSearcher.LeafSlice[]::new);
    }
}
