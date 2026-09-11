/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.Booleans;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.Merger;

import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry of state mergers for derived targets, keyed by the derived
 * data-format category ({@code index.derived.data_format}).
 *
 * <p>A derived target holds state artifacts published by its
 * {@link DerivedArtifactBuilder}, not writer output, and the stock per-format
 * merger cannot compact them without breaking the artifact contract (schema,
 * ordering, footer metadata). The plugin that owns the category therefore
 * supplies a definition-aware {@link Merger} here; the engine hands it to the
 * merge scheduler in place of the stock merger and admits background merges
 * for the target. Without a registered factory (or with compaction switched
 * off) the target keeps its historical behaviour: merges disabled, one
 * segment per published generation.</p>
 *
 * <p>Two switches: the node-wide system property
 * {@link #ENABLED_PROPERTY} (default {@code true}) and the index setting
 * {@link #INDEX_SETTING_KEY} (default {@code true}; registered by the owning
 * plugin, read here by key so the server does not depend on the plugin).</p>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DerivedStateMergers {

    /** Node-wide kill switch for derived-target compaction. */
    public static final String ENABLED_PROPERTY = "opensearch.derived.compaction.enabled";

    /** Per-index switch for derived-target compaction (registered by the owning plugin). */
    public static final String INDEX_SETTING_KEY = "index.derived.compaction.enabled";

    /**
     * Creates the merger for one target shard.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    @FunctionalInterface
    public interface Factory {
        /**
         * @param indexSettings the target index settings
         * @param shardId       the target shard
         * @param shardDataPath the shard's data path (the artifacts live under
         *                      {@code <dataPath>/<format>/})
         * @return the merger, or {@code null} to leave merges disabled for this shard
         */
        Merger create(IndexSettings indexSettings, ShardId shardId, Path shardDataPath);
    }

    private static final Map<String, Factory> FACTORIES = new ConcurrentHashMap<>();

    private DerivedStateMergers() {}

    /**
     * Registers the merger factory for a derived data-format category.
     * Re-registration replaces the previous factory.
     */
    public static void register(String derivedFormatId, Factory factory) {
        FACTORIES.put(Objects.requireNonNull(derivedFormatId, "derivedFormatId"), Objects.requireNonNull(factory, "factory"));
    }

    /** Removes the factory for a category (tests, plugin close). */
    public static void unregister(String derivedFormatId) {
        FACTORIES.remove(derivedFormatId);
    }

    /**
     * Returns the merger for a target shard when compaction is enabled and the
     * category has a registered factory; otherwise empty.
     */
    public static Optional<Merger> create(String derivedFormatId, IndexSettings indexSettings, ShardId shardId, Path shardDataPath) {
        if (derivedFormatId == null) {
            return Optional.empty();
        }
        if (Booleans.parseBoolean(System.getProperty(ENABLED_PROPERTY, Boolean.TRUE.toString())) == false) {
            return Optional.empty();
        }
        if (indexSettings.getSettings().getAsBoolean(INDEX_SETTING_KEY, true) == false) {
            return Optional.empty();
        }
        Factory factory = FACTORIES.get(derivedFormatId);
        if (factory == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(factory.create(indexSettings, shardId, shardDataPath));
    }
}
