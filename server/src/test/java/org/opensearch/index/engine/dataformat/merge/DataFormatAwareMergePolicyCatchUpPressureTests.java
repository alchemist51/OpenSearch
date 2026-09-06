/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.derived.pull.DerivedCatchUpPressure;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Defect #26 at the admission seam: while derived catch-up pressure is active,
 * {@link DataFormatAwareMergePolicy} admits no candidates for ANY data-format
 * shard — scheduled and force selection alike — and admits them again the
 * moment pressure releases. The per-format eligibility predicate keeps working
 * when no pressure is held.
 */
public class DataFormatAwareMergePolicyCatchUpPressureTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("source-index", "uuid"), 0);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        DerivedCatchUpPressure.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        DerivedCatchUpPressure.clearForTests();
        super.tearDown();
    }

    /** Lucene policy that always proposes one merge of every given segment. */
    private MergePolicy selectingPolicy() throws IOException {
        MergePolicy lucenePolicy = mock(MergePolicy.class);
        when(lucenePolicy.findMerges(any(MergeTrigger.class), any(SegmentInfos.class), any())).thenAnswer(inv -> {
            SegmentInfos infos = inv.getArgument(1);
            List<SegmentCommitInfo> all = new ArrayList<>();
            infos.forEach(all::add);
            MergePolicy.MergeSpecification spec = new MergePolicy.MergeSpecification();
            spec.add(new MergePolicy.OneMerge(all));
            return spec;
        });
        when(lucenePolicy.findForcedMerges(any(SegmentInfos.class), anyInt(), any(), any())).thenAnswer(inv -> {
            SegmentInfos infos = inv.getArgument(0);
            List<SegmentCommitInfo> all = new ArrayList<>();
            infos.forEach(all::add);
            MergePolicy.MergeSpecification spec = new MergePolicy.MergeSpecification();
            spec.add(new MergePolicy.OneMerge(all));
            return spec;
        });
        return lucenePolicy;
    }

    private List<Segment> twoSegments() {
        Path tempDir = createTempDir();
        MockDataFormat fmt = new MockDataFormat("lucene", 100L, Set.of());
        WriterFileSet wfs = new WriterFileSet(tempDir.toString(), 1L, Set.of(), 10, 0L);
        Segment seg1 = Segment.builder(1L).addSearchableFiles(fmt, wfs).build();
        Segment seg2 = Segment.builder(2L).addSearchableFiles(fmt, wfs).build();
        return List.of(seg1, seg2);
    }

    public void testScheduledCandidatesDeferredWhilePressureActive() throws IOException {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(selectingPolicy(), SHARD_ID);
        List<Segment> segments = twoSegments();

        assertEquals("baseline: candidates flow with no pressure", 1, policy.findMergeCandidates(segments).size());

        DerivedCatchUpPressure.claim();
        assertTrue(
            "scheduled candidates must be deferred while derived catch-up pressure is active",
            policy.findMergeCandidates(segments).isEmpty()
        );
    }

    public void testForceCandidatesDeferredWhilePressureActive() throws IOException {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(selectingPolicy(), SHARD_ID);
        List<Segment> segments = twoSegments();

        assertEquals("baseline: force candidates flow with no pressure", 1, policy.findForceMergeCandidates(segments, 1).size());

        DerivedCatchUpPressure.claim();
        assertTrue(
            "force candidates must be deferred while derived catch-up pressure is active",
            policy.findForceMergeCandidates(segments, 1).isEmpty()
        );
    }

    public void testCandidatesFlowAgainAfterRelease() throws IOException {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(selectingPolicy(), SHARD_ID);
        List<Segment> segments = twoSegments();

        DerivedCatchUpPressure.claim();
        assertTrue(policy.findMergeCandidates(segments).isEmpty());

        DerivedCatchUpPressure.release();
        assertEquals("candidates must be admitted again after release", 1, policy.findMergeCandidates(segments).size());
    }

    public void testPerFormatPredicateStillAppliedWithoutPressure() throws IOException {
        DataFormatAwareMergePolicy policy = new DataFormatAwareMergePolicy(selectingPolicy(), SHARD_ID, segments -> false);
        List<Segment> segments = twoSegments();

        assertTrue(
            "the injected per-format predicate must keep filtering when no pressure is held",
            policy.findMergeCandidates(segments).isEmpty()
        );
    }
}
