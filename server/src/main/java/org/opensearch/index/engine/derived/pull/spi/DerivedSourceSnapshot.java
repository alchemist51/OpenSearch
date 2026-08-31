/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * Opaque snapshot of a remote derived source (format-agnostic).
 *
 * <p>Represents a point-in-time view of a source shard's published state.
 * Implementations are format-specific (e.g. MV state, vector index) but this
 * interface is intentionally opaque — the generic pull service treats it as
 * an immutable token that flows through the fetch→stage→build pipeline.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DerivedSourceSnapshot {

    /**
     * Returns the shard identifier (index name + shard id) this snapshot
     * refers to, typically the source shard.
     */
    String shardId();

    /**
     * Returns a monotonically increasing watermark that represents the
     * "up-to" position of this snapshot. For MV, this is the max seq-no
     * that has been folded; for other formats it may be a generation or
     * timestamp. The generic service uses this to detect forward progress.
     */
    long watermark();

    /**
     * Returns opaque, format-specific metadata that travels with the
     * snapshot. Committed atomically alongside the built artifact so
     * recovery can resume from the last watermark.
     */
    Map<String, String> metadata();
}
