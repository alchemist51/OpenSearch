/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.merge;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Reproduction tests for defect #22 (merge-eligibility jam after heavy concurrent
 * merge waves, clearable only by restart).
 * <p>
 * Root cause: a merge registration claims its input segments in the
 * currently-merging sets (handler + policy context). Three paths could strand
 * those claims forever, permanently excluding the segments from all future
 * selection until a node restart wiped the in-memory sets:
 * <ol>
 *   <li>an exception mid force-merge wave abandoned the registered-but-not-yet-run
 *       remainder of the wave with no deregistration;</li>
 *   <li>merges completing inside a force-merge wave register follow-up background
 *       merges ({@code onMergeFinished -> findAndRegisterMerges}) into the pending
 *       queue — at idle, no publication/refresh trigger ever drains that queue;</li>
 *   <li>the shutdown short-circuit inside a submitted merge task returned without
 *       resolving the merge's claim (and a task-submission failure leaked the
 *       {@code activeMerges} concurrency budget).</li>
 * </ol>
 * The invariant under test: <b>every registered merge resolves</b> — it either
 * runs (success or failure) or is explicitly deregistered; no segment stays
 * claimed once the scheduler goes quiet, so selection never jams.
 */
public class MergeSchedulerClaimResolutionTests extends OpenSearchTestCase {

    private static final ShardId SHARD_ID = new ShardId(new Index("idx", "uuid"), 0);

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    private static Segment seg(long generation) {
        return new Segment(generation, Map.of());
    }

    private static Supplier<GatedCloseable<CatalogSnapshot>> snapshotWith(List<Segment> segments) {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSegments()).thenReturn(segments);
        return () -> new GatedCloseable<>(snapshot, () -> {});
    }

    private static MergeResult okResult() {
        WriterFileSet files = new WriterFileSet("dir", 99L, Set.of("merged"), 1L, 0L);
        return new MergeResult(Map.of(mock(DataFormat.class), files));
    }

    private IndexSettings indexSettings() {
        return IndexSettingsModule.newIndexSettings(
            "idx",
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build()
        );
    }

    /** Tracks net claims: segments added-but-not-removed from the merging set. */
    private static final class ClaimTrackingListener implements MergeHandler.MergeListener {
        final Set<Segment> claimed = java.util.Collections.synchronizedSet(new HashSet<>());

        @Override
        public void addMergingSegment(Collection<Segment> mergingSegments) {
            claimed.addAll(mergingSegments);
        }

        @Override
        public void removeMergingSegment(Collection<Segment> mergingSegments) {
            claimed.removeAll(mergingSegments);
        }
    }

    /** Policy fed by queues of pre-canned selections; empty queue selects nothing. */
    private static final class ScriptedPolicy implements MergeHandler.MergePolicy {
        final Deque<List<List<Segment>>> forced = new ArrayDeque<>();
        final Deque<List<List<Segment>>> natural = new ArrayDeque<>();

        @Override
        public List<List<Segment>> findMergeCandidates(List<Segment> segments) {
            return natural.isEmpty() ? List.of() : natural.removeFirst();
        }

        @Override
        public List<List<Segment>> findForceMergeCandidates(List<Segment> segments, int maxSegmentCount) {
            return forced.isEmpty() ? List.of() : forced.removeFirst();
        }
    }

    private MergeScheduler newScheduler(MergeHandler handler) {
        return new MergeScheduler(handler, (result, merge) -> {}, () -> {}, SHARD_ID, indexSettings(), threadPool);
    }

    /** Runs scheduler.forceMerge on a FORCE_MERGE pool thread (the production contract). */
    private static IOException forceMergeOnPoolThread(ThreadPool pool, MergeScheduler scheduler, int maxNumSegment) throws Exception {
        java.util.concurrent.CompletableFuture<IOException> outcome = new java.util.concurrent.CompletableFuture<>();
        pool.executor(ThreadPool.Names.FORCE_MERGE).execute(() -> {
            try {
                scheduler.forceMerge(maxNumSegment);
                outcome.complete(null);
            } catch (IOException e) {
                outcome.complete(e);
            } catch (Exception e) {
                outcome.completeExceptionally(e);
            }
        });
        return outcome.get(30, java.util.concurrent.TimeUnit.SECONDS);
    }

    /**
     * Leak path 1: merge N of a force-merge wave fails; the registered
     * remainder of the wave must be deregistered, not stranded.
     */
    public void testForceMergeFailureDeregistersRegisteredRemainder() throws Exception {
        List<Segment> catalog = new ArrayList<>();
        for (long g = 1; g <= 6; g++) {
            catalog.add(seg(g));
        }
        ScriptedPolicy policy = new ScriptedPolicy();
        // One wave of three registered merges: (1,2) fails, (3,4) and (5,6) never run.
        policy.forced.add(
            List.of(List.of(catalog.get(0), catalog.get(1)), List.of(catalog.get(2), catalog.get(3)), List.of(catalog.get(4), catalog.get(5)))
        );
        ClaimTrackingListener claims = new ClaimTrackingListener();
        Merger merger = new Merger() {
            @Override
            public MergeResult merge(MergeInput mergeInput) throws IOException {
                throw new IOException("simulated native merge failure");
            }
        };
        MergeHandler handler = new MergeHandler(snapshotWith(catalog), merger, SHARD_ID, policy, claims, new AtomicLong(100)::incrementAndGet);
        MergeScheduler scheduler = newScheduler(handler);

        IOException failure = forceMergeOnPoolThread(threadPool, scheduler, 1);
        assertNotNull("the failing wave must surface its IOException", failure);

        // Invariant: no segment remains claimed — neither the failed merge's
        // inputs (deregistered by runMerge's failure path) nor the abandoned
        // remainder's (deregistered by the wave's claim-resolution sweep).
        assertBusy(() -> {
            assertTrue("stranded merging claims after failed wave: " + claims.claimed, claims.claimed.isEmpty());
            assertEquals(0, scheduler.getActiveMergeCount());
        });
        assertFalse("no pending merges may survive the wave", scheduler.hasPendingMerges());
    }

    /**
     * Leak path 2: a merge completing inside a force-merge wave registers a
     * follow-up background merge. At idle (no publication trigger), the wave
     * itself must drain that registration; it must not strand in the pending
     * queue with its segments claimed.
     */
    public void testForceMergeDrainsBackgroundMergesRegisteredDuringWave() throws Exception {
        List<Segment> catalog = List.of(seg(1), seg(2), seg(3), seg(4));
        ScriptedPolicy policy = new ScriptedPolicy();
        policy.forced.add(List.of(List.of(catalog.get(0), catalog.get(1))));
        // onMergeFinished -> findAndRegisterMerges: the natural selection made
        // mid-wave — exactly the registration that used to strand at idle.
        policy.natural.add(List.of(List.of(catalog.get(2), catalog.get(3))));
        ClaimTrackingListener claims = new ClaimTrackingListener();
        AtomicInteger mergesRun = new AtomicInteger();
        Merger merger = mergeInput -> {
            mergesRun.incrementAndGet();
            return okResult();
        };
        MergeHandler handler = new MergeHandler(snapshotWith(catalog), merger, SHARD_ID, policy, claims, new AtomicLong(100)::incrementAndGet);
        MergeScheduler scheduler = newScheduler(handler);

        IOException failure = forceMergeOnPoolThread(threadPool, scheduler, 1);
        assertNull(failure);

        // Both the force merge AND the background merge it registered must run.
        assertBusy(() -> {
            assertEquals("background merge registered during the wave must be executed", 2, mergesRun.get());
            assertTrue("stranded merging claims after idle wave: " + claims.claimed, claims.claimed.isEmpty());
            assertFalse("pending queue must drain without any external trigger", scheduler.hasPendingMerges());
            assertEquals(0, scheduler.getActiveMergeCount());
        });
    }

    /**
     * Leak path 1b: shutdown mid-wave abandons the registered remainder — same
     * resolution requirement as the failure path.
     */
    public void testShutdownMidWaveDeregistersRegisteredRemainder() throws Exception {
        List<Segment> catalog = List.of(seg(1), seg(2), seg(3), seg(4));
        ScriptedPolicy policy = new ScriptedPolicy();
        policy.forced.add(List.of(List.of(catalog.get(0), catalog.get(1)), List.of(catalog.get(2), catalog.get(3))));
        ClaimTrackingListener claims = new ClaimTrackingListener();
        MergeScheduler[] schedulerRef = new MergeScheduler[1];
        Merger merger = mergeInput -> {
            // First merge shuts the scheduler down; the loop must then abort
            // AND deregister the second registered merge.
            schedulerRef[0].shutdown();
            return okResult();
        };
        MergeHandler handler = new MergeHandler(snapshotWith(catalog), merger, SHARD_ID, policy, claims, new AtomicLong(100)::incrementAndGet);
        MergeScheduler scheduler = newScheduler(handler);
        schedulerRef[0] = scheduler;

        IOException failure = forceMergeOnPoolThread(threadPool, scheduler, 1);
        assertNull(failure);

        assertBusy(() -> {
            assertTrue("stranded merging claims after shutdown mid-wave: " + claims.claimed, claims.claimed.isEmpty());
            assertEquals(0, scheduler.getActiveMergeCount());
        });
    }

    /** Minimal ExecutorService whose execute() delegates to the given behavior. */
    private static final class ScriptedExecutorService extends java.util.concurrent.AbstractExecutorService {
        private final java.util.function.Consumer<Runnable> onExecute;

        ScriptedExecutorService(java.util.function.Consumer<Runnable> onExecute) {
            this.onExecute = onExecute;
        }

        @Override
        public void execute(Runnable command) {
            onExecute.accept(command);
        }

        @Override
        public void shutdown() {}

        @Override
        public List<Runnable> shutdownNow() {
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) {
            return true;
        }
    }

    /**
     * Leak path 3: the shutdown short-circuit inside a submitted merge task
     * must resolve the merge's claim instead of returning with it held.
     */
    public void testShutdownShortCircuitInsideTaskResolvesClaim() throws Exception {
        List<Segment> catalog = List.of(seg(1), seg(2));
        ScriptedPolicy policy = new ScriptedPolicy();
        policy.natural.add(List.of(List.of(catalog.get(0), catalog.get(1))));
        ClaimTrackingListener claims = new ClaimTrackingListener();
        Merger merger = mergeInput -> okResult();
        MergeHandler handler = new MergeHandler(snapshotWith(catalog), merger, SHARD_ID, policy, claims, new AtomicLong(100)::incrementAndGet);

        // Deferred executor: capture the merge task, flip shutdown, then run it.
        List<Runnable> captured = java.util.Collections.synchronizedList(new ArrayList<>());
        ThreadPool deferring = mock(ThreadPool.class);
        when(deferring.executor(ThreadPool.Names.MERGE)).thenReturn(new ScriptedExecutorService(captured::add));
        MergeScheduler scheduler = new MergeScheduler(handler, (result, merge) -> {}, () -> {}, SHARD_ID, indexSettings(), deferring);

        scheduler.triggerMerges();
        assertEquals("merge task must have been submitted", 1, captured.size());
        assertFalse("claim must be held while the task is queued", claims.claimed.isEmpty());

        scheduler.shutdown();
        captured.get(0).run();

        assertTrue("shutdown short-circuit must resolve the merge's claim: " + claims.claimed, claims.claimed.isEmpty());
        assertEquals(0, scheduler.getActiveMergeCount());
    }

    /**
     * Leak path 3b: a task-submission failure must not leak the activeMerges
     * concurrency budget (a leaked increment permanently shrinks the number of
     * merges the scheduler will ever run again — the same restart-only cure).
     */
    public void testSubmissionFailureRestoresConcurrencyBudgetAndClaim() throws Exception {
        List<Segment> catalog = List.of(seg(1), seg(2));
        ScriptedPolicy policy = new ScriptedPolicy();
        policy.natural.add(List.of(List.of(catalog.get(0), catalog.get(1))));
        ClaimTrackingListener claims = new ClaimTrackingListener();
        Merger merger = mergeInput -> okResult();
        MergeHandler handler = new MergeHandler(snapshotWith(catalog), merger, SHARD_ID, policy, claims, new AtomicLong(100)::incrementAndGet);

        ThreadPool rejecting = mock(ThreadPool.class);
        when(rejecting.executor(ThreadPool.Names.MERGE)).thenReturn(new ScriptedExecutorService(task -> {
            throw new org.opensearch.core.concurrency.OpenSearchRejectedExecutionException("pool saturated");
        }));
        MergeScheduler scheduler = new MergeScheduler(handler, (result, merge) -> {}, () -> {}, SHARD_ID, indexSettings(), rejecting);

        scheduler.triggerMerges();

        assertEquals("rejected submission must roll back the concurrency budget", 0, scheduler.getActiveMergeCount());
        assertTrue("rejected submission must resolve the merge's claim: " + claims.claimed, claims.claimed.isEmpty());
        assertFalse(scheduler.hasPendingMerges());
    }
}
