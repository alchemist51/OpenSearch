/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level static registry of per-shard {@link MVStateRefreshListener}s.
 *
 * <p>Lives in the server module so both mv-engine (transport handler registration)
 * and parquet-data-format (gen resume at writer open) can access it without
 * cross-plugin dependencies. Same pattern as {@link org.opensearch.index.engine.dataformat.MVWriterConfigRegistry}.</p>
 *
 * <p>Listeners are registered when a shard with MV definitions opens and
 * unregistered when the shard closes.</p>
 *
 * @opensearch.internal
 */
public final class MVCheckpointService {

    private static final Logger logger = LogManager.getLogger(MVCheckpointService.class);

    public static final String ACTION_GET_CHECKPOINT = "internal:index/shard/mv/get_checkpoint";

    private static final ConcurrentHashMap<ShardId, MVStateRefreshListener> LISTENERS = new ConcurrentHashMap<>();

    private MVCheckpointService() {}

    /**
     * Register a shard's refresh listener. Called when a shard with MV definitions opens.
     */
    public static void registerListener(ShardId shardId, MVStateRefreshListener listener) {
        LISTENERS.put(shardId, listener);
        logger.debug("MVCheckpointService: registered listener for shard={}", shardId);
    }

    /**
     * Unregister a shard's refresh listener. Called when a shard closes.
     */
    public static void unregisterListener(ShardId shardId) {
        LISTENERS.remove(shardId);
        logger.debug("MVCheckpointService: unregistered listener for shard={}", shardId);
    }

    /**
     * Get the listener for a shard. Used by gen resume wiring and transport handler.
     */
    public static MVStateRefreshListener getListener(ShardId shardId) {
        return LISTENERS.get(shardId);
    }

    /**
     * Get the checkpoint for a shard. Used by the transport handler.
     * Returns an empty checkpoint if no listener is registered.
     */
    public static MVCheckpoint getCheckpoint(ShardId shardId) {
        MVStateRefreshListener listener = LISTENERS.get(shardId);
        if (listener == null) {
            return new MVCheckpoint(shardId, 0, -1, 0, Map.of());
        }
        return listener.getLastCheckpoint();
    }

    /**
     * Get all registered shard IDs (for diagnostics).
     */
    public static java.util.Set<ShardId> registeredShards() {
        return java.util.Collections.unmodifiableSet(LISTENERS.keySet());
    }
}
