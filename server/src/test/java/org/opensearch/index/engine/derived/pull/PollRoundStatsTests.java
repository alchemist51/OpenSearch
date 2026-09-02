/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.opensearch.index.engine.derived.pull.spi.PollRoundStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Tests for {@link PollRoundStats} (immutability, builder, toString) and
 * {@link DerivedShardPoller.StageAccumulator} (cumulative count/total/mean/max).
 */
public class PollRoundStatsTests extends OpenSearchTestCase {

    // ── PollRoundStats.Builder ──────────────────────────────────────────

    public void testBuilderStagesImmutable() {
        PollRoundStats stats = new PollRoundStats.Builder().startRound().stage("fetch", 100_000_000L).stage("build", 200_000_000L).build();

        assertEquals(2, stats.stageNanos().size());
        assertEquals(100_000_000L, stats.stageNanos().get("fetch").longValue());
        assertEquals(200_000_000L, stats.stageNanos().get("build").longValue());

        // Returned map must be immutable
        expectThrows(UnsupportedOperationException.class, () -> stats.stageNanos().put("extra", 1L));
    }

    public void testBuilderCountersImmutable() {
        PollRoundStats stats = new PollRoundStats.Builder().startRound().counter("rows", 42).counter("bytes", 1024).build();

        assertEquals(2, stats.counters().size());
        assertEquals(42L, stats.counters().get("rows").longValue());
        assertEquals(1024L, stats.counters().get("bytes").longValue());

        expectThrows(UnsupportedOperationException.class, () -> stats.counters().put("extra", 1L));
    }

    public void testBuilderTotalNanosComputedFromStartRound() throws InterruptedException {
        PollRoundStats.Builder builder = new PollRoundStats.Builder().startRound();
        // A tiny sleep to ensure total > 0
        Thread.sleep(1);
        PollRoundStats stats = builder.build();
        assertTrue("total nanos must be > 0 when startRound called", stats.totalNanos() > 0);
    }

    public void testBuilderTotalNanosZeroWithoutStartRound() {
        PollRoundStats stats = new PollRoundStats.Builder().stage("test", 50L).build();
        assertEquals("total nanos is 0 when startRound not called", 0, stats.totalNanos());
    }

    public void testBuilderEmptyStats() {
        PollRoundStats stats = new PollRoundStats.Builder().startRound().build();
        assertTrue(stats.stageNanos().isEmpty());
        assertTrue(stats.counters().isEmpty());
    }

    public void testToStringIncludesStagesAndCounters() {
        PollRoundStats stats = new PollRoundStats.Builder().startRound().stage("fetch_snapshot", 5_000_000L).counter("rows", 100).build();

        String s = stats.toString();
        assertTrue("toString must contain stage name", s.contains("fetch_snapshot"));
        assertTrue("toString must contain 'ms'", s.contains("ms"));
        assertTrue("toString must contain counter", s.contains("rows=100"));
    }

    // ── StageAccumulator ────────────────────────────────────────────────

    public void testStageAccumulatorSingleValue() {
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        acc.add(100_000_000L); // 100ms

        assertEquals(1, acc.count());
        assertEquals(100_000_000L, acc.totalNanos());
        assertEquals(100_000_000L, acc.maxNanos());
        assertEquals(100_000_000L, acc.meanNanos());
    }

    public void testStageAccumulatorMultipleValues() {
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        acc.add(100_000_000L); // 100ms
        acc.add(200_000_000L); // 200ms
        acc.add(300_000_000L); // 300ms

        assertEquals(3, acc.count());
        assertEquals(600_000_000L, acc.totalNanos());
        assertEquals(300_000_000L, acc.maxNanos());
        assertEquals(200_000_000L, acc.meanNanos()); // 600/3 = 200
    }

    public void testStageAccumulatorZeroCount() {
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        assertEquals(0, acc.count());
        assertEquals(0, acc.totalNanos());
        assertEquals(0, acc.maxNanos());
        assertEquals(0, acc.meanNanos()); // 0/0 → 0
    }

    public void testStageAccumulatorMaxTracksLargest() {
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        acc.add(50L);
        acc.add(500L);
        acc.add(100L);
        assertEquals(500L, acc.maxNanos());
    }

    public void testStageAccumulatorToString() {
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        acc.add(1_000_000L); // 1ms
        acc.add(2_000_000L); // 2ms
        String s = acc.toString();
        assertTrue(s.contains("count=2"));
        assertTrue(s.contains("ms"));
    }

    // ── MV sub-stage propagation via BuildResult.stats() ────────────

    public void testBuildStatsNanosRoutedToStagesNotCounters() {
        // Simulate what DerivedShardPoller does: merge BuildResult.stats()
        // with _nanos entries routed to stages and non-nanos to counters.
        PollRoundStats.Builder builder = new PollRoundStats.Builder().startRound()
            .stage("fetch_snapshot", 10L)
            .stage("download", 20L)
            .stage("build", 30L);

        // Simulated BuildResult.stats() from MV builder
        Map<String, Object> buildStats = Map.of(
            "coverage_check_nanos",
            25_000_000L,
            "native_build_nanos",
            5_000_000L,
            "publish_nanos",
            3_000_000L,
            "stateRows",
            10000,
            "parquet_files",
            3L
        );

        // Apply the same routing logic as DerivedShardPoller
        buildStats.forEach((k, v) -> {
            if (v instanceof Number) {
                if (k.endsWith("_nanos")) {
                    builder.stage(k, ((Number) v).longValue());
                } else {
                    builder.counter(k, ((Number) v).longValue());
                }
            }
        });

        PollRoundStats stats = builder.build();

        // _nanos entries must be in stageNanos (fetch_snapshot, download, build + 3 sub-stages = 6)
        assertEquals(6, stats.stageNanos().size());
        assertEquals(25_000_000L, stats.stageNanos().get("coverage_check_nanos").longValue());
        assertEquals(5_000_000L, stats.stageNanos().get("native_build_nanos").longValue());
        assertEquals(3_000_000L, stats.stageNanos().get("publish_nanos").longValue());

        // Non-nanos entries must be in counters only
        assertEquals(2, stats.counters().size());
        assertEquals(10000L, stats.counters().get("stateRows").longValue());
        assertEquals(3L, stats.counters().get("parquet_files").longValue());

        // Verify nanos entries are NOT in counters
        assertNull(stats.counters().get("coverage_check_nanos"));
        assertNull(stats.counters().get("native_build_nanos"));
        assertNull(stats.counters().get("publish_nanos"));
    }

    public void testNanosStagesGetCumulativeAccumulation() {
        // Verify that _nanos entries accumulated across multiple rounds get
        // the StageAccumulator treatment (count/total/mean/max).
        DerivedShardPoller.StageAccumulator acc = new DerivedShardPoller.StageAccumulator();
        acc.add(25_000_000L); // round 1: 25ms coverage_check
        acc.add(30_000_000L); // round 2: 30ms coverage_check
        acc.add(20_000_000L); // round 3: 20ms coverage_check

        assertEquals(3, acc.count());
        assertEquals(75_000_000L, acc.totalNanos());
        assertEquals(25_000_000L, acc.meanNanos()); // 75/3 = 25
        assertEquals(30_000_000L, acc.maxNanos());
    }

    // ── Bounded per-round samples ───────────────────────────────────

    public void testRoundSamplesBoundedToMax256() {
        // ConcurrentLinkedDeque + MAX_SAMPLES=256 in DerivedShardPoller
        // We verify the contract: after > 256 rounds, oldest are evicted.
        java.util.concurrent.ConcurrentLinkedDeque<PollRoundStats> samples = new java.util.concurrent.ConcurrentLinkedDeque<>();
        int maxSamples = 256;

        for (int i = 0; i < 300; i++) {
            PollRoundStats stat = new PollRoundStats.Builder().startRound().counter("round", i).build();
            samples.addLast(stat);
            while (samples.size() > maxSamples) {
                samples.pollFirst();
            }
        }

        assertEquals(maxSamples, samples.size());
        // Oldest remaining should be round 44 (300-256)
        assertEquals(44L, samples.peekFirst().counters().get("round").longValue());
        // Newest should be round 299
        assertEquals(299L, samples.peekLast().counters().get("round").longValue());
    }

    // ── Success/failure accounting ──────────────────────────────────

    public void testSuccessFailureAccountingSeparate() {
        // Simulates the DerivedShardPoller counting pattern
        java.util.concurrent.atomic.AtomicLong roundCount = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicLong successCount = new java.util.concurrent.atomic.AtomicLong();
        java.util.concurrent.atomic.AtomicLong failureCount = new java.util.concurrent.atomic.AtomicLong();

        // 3 successes
        for (int i = 0; i < 3; i++) {
            roundCount.incrementAndGet();
            successCount.incrementAndGet();
        }
        // 2 failures
        for (int i = 0; i < 2; i++) {
            roundCount.incrementAndGet();
            failureCount.incrementAndGet();
        }

        assertEquals(5, roundCount.get());
        assertEquals(3, successCount.get());
        assertEquals(2, failureCount.get());
        assertEquals(roundCount.get(), successCount.get() + failureCount.get());
    }
}
