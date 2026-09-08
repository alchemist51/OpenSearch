/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.core.index.shard.ShardId;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Static factory registry for creating MV refresh listeners on source shards.
 *
 * <p>Lives in the server module so IndexShard.newEngineConfig() can call it
 * without depending on the mv-engine plugin. The plugin deposits a factory
 * in createComponents(); IndexShard calls it when building the engine for a
 * shard whose IndexMetadata customData contains {@code mv_definitions}.</p>
 *
 * <p>Same static-registry pattern as {@link MVCheckpointService} and
 * {@link org.opensearch.index.engine.dataformat.MVWriterConfigRegistry}.</p>
 *
 * @opensearch.internal
 */
public final class MVRefreshListenerFactory {

    /**
     * Factory that creates an MVStateRefreshListener for a source shard.
     */
    @FunctionalInterface
    public interface Factory {
        /**
         * Create and return a refresh listener for the given source shard.
         *
         * @param shardId       the source shard
         * @param shardDataPath data path for the shard
         * @param primaryTerm   current primary term
         * @param mvDefinitions map of mvId -> definitionHash from customData
         * @param processedCheckpointSupplier supplier for the shard's processedLocalCheckpoint
         * @return the refresh listener, or null if not applicable
         */
        ReferenceManager.RefreshListener create(
            ShardId shardId,
            Path shardDataPath,
            long primaryTerm,
            Map<String, String> mvDefinitions,
            LongSupplier processedCheckpointSupplier
        ) throws Exception;
    }

    private static volatile Factory factory;

    private MVRefreshListenerFactory() {}

    /**
     * Register the factory. Called once by the MV plugin at startup.
     */
    public static void register(Factory f) {
        factory = f;
    }

    /**
     * Unregister the factory. Called on plugin close.
     */
    public static void unregister() {
        factory = null;
    }

    /**
     * Create a refresh listener for the given shard, or null if no factory registered
     * or the factory returns null.
     */
    public static ReferenceManager.RefreshListener create(
        ShardId shardId,
        Path shardDataPath,
        long primaryTerm,
        Map<String, String> mvDefinitions,
        LongSupplier processedCheckpointSupplier
    ) {
        Factory f = factory;
        if (f == null) {
            return null;
        }
        try {
            return f.create(shardId, shardDataPath, primaryTerm, mvDefinitions, processedCheckpointSupplier);
        } catch (Exception e) {
            org.apache.logging.log4j.LogManager.getLogger(MVRefreshListenerFactory.class)
                .warn("Failed to create MV refresh listener for shard {}: {}", shardId, e.getMessage());
            return null;
        }
    }
}
