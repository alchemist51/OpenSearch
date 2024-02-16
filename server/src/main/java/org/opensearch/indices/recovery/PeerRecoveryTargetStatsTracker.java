/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import java.util.concurrent.atomic.AtomicLong;

public class PeerRecoveryTargetStatsTracker {
    private AtomicLong totalShardRelocation;

    public PeerRecoveryTargetStatsTracker() {
        totalShardRelocation = new AtomicLong();
    }

    public long getTotalShardRelocation() {
        return totalShardRelocation.get();
    }

    public void incrementTotalShardRelocation(long increment) {
        totalShardRelocation.addAndGet(increment);
    }
}
