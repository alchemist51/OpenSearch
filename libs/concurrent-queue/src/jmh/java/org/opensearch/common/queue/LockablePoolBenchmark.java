/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.queue;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JMH benchmark for {@link LockablePool} measuring:
 * <ul>
 *   <li>Isolated checkout/return throughput at varying thread counts</li>
 *   <li>Mixed workload: concurrent writers + periodic checkoutAll (refresh)</li>
 * </ul>
 * The mixed group benchmark is the most realistic — it models the composite
 * engine's write path where indexing threads hold writers while a refresh
 * thread periodically drains the pool via checkoutAll.
 */
@Fork(2)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class LockablePoolBenchmark {

    @Param({ "4", "8" })
    int concurrency;

    private LockablePool<PoolEntry> pool;

    @Setup(Level.Iteration)
    public void setup() {
        AtomicInteger counter = new AtomicInteger(0);
        pool = new LockablePool<>(() -> new PoolEntry(counter.getAndIncrement()), LinkedList::new, concurrency);
        // Pre-warm the pool
        for (int i = 0; i < concurrency * 2; i++) {
            PoolEntry e = pool.getAndLock();
            pool.releaseAndUnlock(e);
        }
    }

    // ---- Mixed workload: writers + periodic refresh (checkoutAll) ----
    // This is the realistic scenario: multiple indexing threads hold writers
    // while a single refresh thread periodically drains the pool.

    @Benchmark
    @Group("mixed_7w_1r")
    @GroupThreads(7)
    public void writers_7w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("mixed_7w_1r")
    @GroupThreads(1)
    public List<PoolEntry> refresh_7w1r() throws InterruptedException {
        Thread.sleep(1000); // simulate 1s refresh interval
        return pool.checkoutAll();
    }

    @Benchmark
    @Group("mixed_3w_1r")
    @GroupThreads(3)
    public void writers_3w1r(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("mixed_3w_1r")
    @GroupThreads(1)
    public List<PoolEntry> refresh_3w1r() throws InterruptedException {
        Thread.sleep(1000); // simulate 1s refresh interval
        return pool.checkoutAll();
    }

    // ---- Isolated: pure writer throughput (no refresh contention) ----

    @Benchmark
    @Group("writers_only_4t")
    @GroupThreads(4)
    public void writersOnly_4t(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    @Benchmark
    @Group("writers_only_8t")
    @GroupThreads(8)
    public void writersOnly_8t(Blackhole bh) {
        PoolEntry e = pool.getAndLock();
        bh.consume(simulateWork(e));
        pool.releaseAndUnlock(e);
    }

    private static long simulateWork(PoolEntry entry) {
        long result = entry.hashCode();
        for (int i = 0; i < 20; i++) {
            result ^= (result << 13);
            result ^= (result >> 7);
            result ^= (result << 17);
        }
        return result;
    }

    static final class PoolEntry implements Lockable {
        final int id;
        private final ReentrantLock lock = new ReentrantLock();

        PoolEntry(int id) {
            this.id = id;
        }

        @Override
        public void lock() {
            lock.lock();
        }

        @Override
        public boolean tryLock() {
            return lock.tryLock();
        }

        @Override
        public void unlock() {
            lock.unlock();
        }
    }
}
