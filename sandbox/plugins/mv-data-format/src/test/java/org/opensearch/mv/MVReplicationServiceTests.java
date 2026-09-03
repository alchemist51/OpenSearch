/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link MVReplicationService} (request-driven model).
 *
 * <p>In the request-driven model, MVReplicationService is gutted to:
 * noop tracking ownership + shard close cleanup. The scheduled tick,
 * TrackedShard, reconciliation, publisher wiring — all deleted.</p>
 *
 * <ul>
 *   <li>Owns and exposes MVNoopTracker</li>
 *   <li>Cleans up noop data on shard close via afterIndexShardClosed</li>
 *   <li>Lifecycle: close is safe and idempotent</li>
 * </ul>
 */
public class MVReplicationServiceTests extends OpenSearchTestCase {

    private static final ShardId SHARD_A = new ShardId(new Index("source-a", "uuid-a"), 0);
    private static final ShardId SHARD_B = new ShardId(new Index("source-b", "uuid-b"), 0);

    // ── Noop tracker ownership ───────────────────────────────────────────

    public void testNoopTrackerOwned() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVReplicationService service = new MVReplicationService(tracker);
        assertSame(tracker, service.noopTracker());
    }

    // ── afterIndexShardClosed removes noop data ──────────────────────────

    public void testAfterIndexShardClosedRemovesNoopData() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD_A, 10L);
        tracker.recordNoop(SHARD_A, 20L);
        tracker.recordNoop(SHARD_B, 30L);
        assertEquals(2, tracker.trackedCount(SHARD_A));
        assertEquals(1, tracker.trackedCount(SHARD_B));
        assertEquals(2, tracker.trackedShardCount());

        MVReplicationService service = new MVReplicationService(tracker);
        service.afterIndexShardClosed(SHARD_A, null, Settings.EMPTY);

        assertEquals(0, tracker.trackedCount(SHARD_A));
        assertEquals(1, tracker.trackedCount(SHARD_B));
        assertEquals(1, tracker.trackedShardCount());
    }

    public void testAfterIndexShardClosedNullTrackerIsNoop() {
        // Null tracker should not throw
        MVReplicationService service = new MVReplicationService(null);
        service.afterIndexShardClosed(SHARD_A, null, Settings.EMPTY);
        // Just verifying no exception
    }

    public void testAfterIndexShardClosedUnknownShardIsNoop() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVReplicationService service = new MVReplicationService(tracker);
        // Closing a shard that was never tracked should not throw
        service.afterIndexShardClosed(SHARD_A, null, Settings.EMPTY);
        assertEquals(0, tracker.trackedShardCount());
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    public void testCloseIsIdempotent() {
        MVNoopTracker tracker = new MVNoopTracker();
        MVReplicationService service = new MVReplicationService(tracker);
        assertFalse(service.isClosed());
        service.close();
        assertTrue(service.isClosed());
        service.close(); // second close is safe
        assertTrue(service.isClosed());
    }

    public void testCloseDoesNotAffectTrackerData() {
        MVNoopTracker tracker = new MVNoopTracker();
        tracker.recordNoop(SHARD_A, 10L);
        MVReplicationService service = new MVReplicationService(tracker);
        service.close();
        // Tracker data survives service close (handler still needs it
        // for in-flight requests during graceful shutdown)
        assertEquals(1, tracker.trackedCount(SHARD_A));
    }
}
