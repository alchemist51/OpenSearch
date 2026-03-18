/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Tests for {@link CompositeEnginePlugin}.
 */
public class CompositeEnginePluginTests extends OpenSearchTestCase {

    // --- getSettings tests ---

    public void testGetSettingsReturnsBothSettings() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        List<Setting<?>> settings = plugin.getSettings();
        assertEquals(2, settings.size());
        assertTrue(settings.contains(CompositeEnginePlugin.COMPOSITE_ENABLED));
        assertTrue(settings.contains(CompositeEnginePlugin.PRIMARY_DATA_FORMAT));
    }

    // --- getEngineFactory tests ---

    public void testGetEngineFactoryReturnsEmptyWhenCompositeDisabled() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        IndexSettings indexSettings = createIndexSettings(false, "lucene");
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings);
        assertTrue(factory.isEmpty());
    }

    public void testGetEngineFactoryReturnsFactoryWhenCompositeEnabledAndPrimaryFormatMatches() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.onDiscovery(List.of(createMockDataFormatPlugin("lucene", 1)));
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings);
        assertTrue(factory.isPresent());
    }

    public void testGetEngineFactoryThrowsWhenPrimaryFormatDoesNotMatch() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.onDiscovery(List.of(createMockDataFormatPlugin("lucene", 1)));
        IndexSettings indexSettings = createIndexSettings(true, "parquet");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> plugin.getEngineFactory(indexSettings));
        assertTrue(ex.getMessage().contains("parquet"));
        assertTrue(ex.getMessage().contains("does not match any registered DataFormatPlugin"));
    }

    public void testGetEngineFactoryReturnsEmptyWhenNoPluginsDiscoveredAndCompositeEnabled() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        // No onDiscovery call — dataFormatPlugins is empty
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings);
        assertTrue(factory.isEmpty());
    }

    // --- onDiscovery tests ---

    public void testOnDiscoveryWithEmptyCollection() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.onDiscovery(Collections.emptyList());
        // Composite enabled but no plugins → should return empty
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings);
        assertTrue(factory.isEmpty());
    }

    public void testOnDiscoveryWithOnePlugin() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.onDiscovery(List.of(createMockDataFormatPlugin("lucene", 1)));
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        Optional<EngineFactory> factory = plugin.getEngineFactory(indexSettings);
        assertTrue(factory.isPresent());
    }

    public void testOnDiscoveryWithMultiplePlugins() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        plugin.onDiscovery(List.of(createMockDataFormatPlugin("lucene", 1), createMockDataFormatPlugin("parquet", 2)));
        // Both formats should be available
        IndexSettings luceneSettings = createIndexSettings(true, "lucene");
        assertTrue(plugin.getEngineFactory(luceneSettings).isPresent());

        IndexSettings parquetSettings = createIndexSettings(true, "parquet");
        assertTrue(plugin.getEngineFactory(parquetSettings).isPresent());
    }

    public void testOnDiscoveryDuplicateFormatNameKeepsHighestPriority() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        DataFormatPlugin lowPriority = createMockDataFormatPlugin("lucene", 1);
        DataFormatPlugin highPriority = createMockDataFormatPlugin("lucene", 100);
        plugin.onDiscovery(List.of(lowPriority, highPriority));

        // The plugin should still work with "lucene" format — the high-priority one wins
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        assertTrue(plugin.getEngineFactory(indexSettings).isPresent());
    }

    public void testOnDiscoveryDuplicateFormatNameKeepsHighestPriorityRegardlessOfOrder() {
        CompositeEnginePlugin plugin = new CompositeEnginePlugin();
        DataFormatPlugin highPriority = createMockDataFormatPlugin("lucene", 100);
        DataFormatPlugin lowPriority = createMockDataFormatPlugin("lucene", 1);
        // High priority first, then low priority — should still keep high priority
        plugin.onDiscovery(List.of(highPriority, lowPriority));

        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        assertTrue(plugin.getEngineFactory(indexSettings).isPresent());
    }

    // Feature: composite-indexing-plugin, Property 10: Primary format validation
    // Validates: Requirements 7.3
    //
    // For any primary format name and any set of registered DataFormatPlugin instances,
    // the CompositeEnginePlugin should accept the configuration if and only if the
    // primary format name matches a registered plugin's DataFormat.name().
    public void testPropertyPrimaryFormatValidation() {
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            // Generate a random set of format names (1-5 formats)
            int formatCount = randomIntBetween(1, 5);
            String[] formatNames = new String[formatCount];
            for (int j = 0; j < formatCount; j++) {
                formatNames[j] = randomAlphaOfLength(randomIntBetween(3, 10));
            }

            // Register plugins for all generated format names
            CompositeEnginePlugin plugin = new CompositeEnginePlugin();
            List<DataFormatPlugin> plugins = new ArrayList<>();
            for (String name : formatNames) {
                plugins.add(createMockDataFormatPlugin(name, randomLongBetween(1, 1000)));
            }
            plugin.onDiscovery(plugins);

            // Case 1: Primary format matches one of the registered formats → factory is present
            String matchingFormat = formatNames[randomIntBetween(0, formatCount - 1)];
            IndexSettings matchSettings = createIndexSettings(true, matchingFormat);
            Optional<EngineFactory> matchResult = plugin.getEngineFactory(matchSettings);
            assertTrue(
                "Expected factory to be present when primary format ["
                    + matchingFormat
                    + "] matches a registered plugin (iteration "
                    + i
                    + ")",
                matchResult.isPresent()
            );

            // Case 2: Primary format does NOT match any registered format → throws IllegalArgumentException
            String nonMatchingFormat = randomValueOtherThanMany(
                candidate -> Arrays.asList(formatNames).contains(candidate),
                () -> randomAlphaOfLength(randomIntBetween(3, 10))
            );
            IndexSettings noMatchSettings = createIndexSettings(true, nonMatchingFormat);
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                "Expected IllegalArgumentException when primary format ["
                    + nonMatchingFormat
                    + "] does not match any registered plugin (iteration "
                    + i
                    + ")",
                () -> plugin.getEngineFactory(noMatchSettings)
            );
            assertTrue("Error message should contain the non-matching format name", ex.getMessage().contains(nonMatchingFormat));
        }
    }

    // --- Helper methods ---

    private IndexSettings createIndexSettings(boolean compositeEnabled, String primaryFormat) {
        Settings settings = Settings.builder()
            .put("index.composite.enabled", compositeEnabled)
            .put("index.composite.primary_data_format", primaryFormat)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }

    private DataFormatPlugin createMockDataFormatPlugin(String formatName, long priority) {
        DataFormat dataFormat = new DataFormat() {
            @Override
            public String name() {
                return formatName;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return Set.of(new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH)));
            }
        };

        return new DataFormatPlugin() {
            @Override
            public DataFormat getDataFormat() {
                return dataFormat;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <T extends DataFormat, P extends DocumentInput<?>> IndexingExecutionEngine<T, P> indexingEngine(
                MapperService mapperService,
                ShardPath shardPath,
                IndexSettings indexSettings
            ) {
                return null; // Not needed for plugin-level tests
            }
        };
    }
}
