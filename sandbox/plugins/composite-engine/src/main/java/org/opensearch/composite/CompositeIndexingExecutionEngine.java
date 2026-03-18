/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A composite {@link IndexingExecutionEngine} that orchestrates indexing across
 * multiple per-format engines behind a single interface.
 * <p>
 * The engine delegates writer creation, refresh, file deletion, and document input
 * creation to each per-format engine. A primary engine is designated based on the
 * configured primary format name and is used for merge operations.
 * <p>
 * The composite {@link DataFormat} exposed by this engine represents the union of
 * all per-format supported field type capabilities.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<CompositeDataFormat, CompositeDocumentInput> {

    private static final Logger logger = LogManager.getLogger(CompositeIndexingExecutionEngine.class);

    private final IndexingExecutionEngine<?, ?> primaryEngine;
    private final List<IndexingExecutionEngine<?, ?>> secondaryEngines;
    private final List<IndexingExecutionEngine<?, ?>> allEngines;
    private final CompositeDataFormat compositeDataFormat;

    /**
     * Constructs a CompositeIndexingExecutionEngine by reading index settings to
     * determine the primary and secondary data formats, validating that all configured
     * formats are registered, and creating per-format engines via the discovered
     * {@link DataFormatPlugin} instances.
     * <p>
     * The primary engine is the authoritative format used for merge operations and
     * commit coordination. Secondary engines receive writes alongside the primary but
     * are not used as the merge authority.
     *
     * @param dataFormatPlugins the discovered data format plugins keyed by format name
     * @param indexSettings the index settings containing composite configuration
     * @param mapperService the mapper service for field mapping resolution
     * @param shardPath the shard path for file storage
     * @throws IllegalStateException if composite indexing is not enabled
     * @throws IllegalArgumentException if any configured format is not registered
     */
    public CompositeIndexingExecutionEngine(
        Map<String, DataFormatPlugin> dataFormatPlugins,
        IndexSettings indexSettings,
        MapperService mapperService,
        ShardPath shardPath
    ) {
        Objects.requireNonNull(dataFormatPlugins, "dataFormatPlugins must not be null");
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");

        Settings settings = indexSettings.getSettings();
        boolean compositeEnabled = CompositeEnginePlugin.COMPOSITE_ENABLED.get(settings);
        if (compositeEnabled == false) {
            throw new IllegalStateException(
                "Composite indexing is not enabled for index [" + indexSettings.getIndex().getName() + "]"
            );
        }

        String primaryFormatName = CompositeEnginePlugin.PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = CompositeEnginePlugin.SECONDARY_DATA_FORMATS.get(settings);

        validateFormatsRegistered(dataFormatPlugins, primaryFormatName, secondaryFormatNames);

        DataFormatPlugin primaryPlugin = dataFormatPlugins.get(primaryFormatName);
        this.primaryEngine = primaryPlugin.indexingEngine(mapperService, shardPath, indexSettings);

        List<IndexingExecutionEngine<?, ?>> secondaries = new ArrayList<>();
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName.equals(primaryFormatName)) {
                logger.warn("Secondary data format [{}] is the same as primary, skipping duplicate", secondaryName);
                continue;
            }
            DataFormatPlugin secondaryPlugin = dataFormatPlugins.get(secondaryName);
            secondaries.add(secondaryPlugin.indexingEngine(mapperService, shardPath, indexSettings));
        }
        this.secondaryEngines = List.copyOf(secondaries);

        List<IndexingExecutionEngine<?, ?>> all = new ArrayList<>();
        all.add(this.primaryEngine);
        all.addAll(this.secondaryEngines);
        this.allEngines = List.copyOf(all);

        List<DataFormat> allFormats = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> engine : this.allEngines) {
            allFormats.add(engine.getDataFormat());
        }
        this.compositeDataFormat = new CompositeDataFormat(allFormats, this.primaryEngine.getDataFormat());
    }

    /**
     * Validates that the primary and all secondary data format plugins are registered.
     *
     * @param dataFormatPlugins the discovered data format plugins keyed by format name
     * @param primaryFormatName the configured primary format name
     * @param secondaryFormatNames the configured secondary format names
     * @throws IllegalArgumentException if any configured format is not registered
     */
    static void validateFormatsRegistered(
        Map<String, DataFormatPlugin> dataFormatPlugins,
        String primaryFormatName,
        List<String> secondaryFormatNames
    ) {
        if (dataFormatPlugins.containsKey(primaryFormatName) == false) {
            throw new IllegalArgumentException(
                "Primary data format ["
                    + primaryFormatName
                    + "] is not registered on this node. Available formats: "
                    + dataFormatPlugins.keySet()
            );
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName.equals(primaryFormatName)) {
                continue;
            }
            if (dataFormatPlugins.containsKey(secondaryName) == false) {
                throw new IllegalArgumentException(
                    "Secondary data format ["
                        + secondaryName
                        + "] is not registered on this node. Available formats: "
                        + dataFormatPlugins.keySet()
                );
            }
        }
    }

    @Override
    public Writer<CompositeDocumentInput> createWriter(long writerGeneration) {
        Map<DataFormat, Writer<?>> writerMap = new LinkedHashMap<>();
        for (IndexingExecutionEngine<?, ?> engine : allEngines) {
            Writer<?> writer = engine.createWriter(writerGeneration);
            writerMap.put(engine.getDataFormat(), writer);
        }
        return new CompositeWriter(writerMap);
    }

    @Override
    public Merger getMerger() {
        return primaryEngine.getMerger();
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        List<Segment> allSegments = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> engine : allEngines) {
            RefreshResult result = engine.refresh(refreshInput);
            allSegments.addAll(result.refreshedSegments());
        }
        return new RefreshResult(allSegments);
    }

    @Override
    public CompositeDataFormat getDataFormat() {
        return compositeDataFormat;
    }

    @Override
    public long getNativeBytesUsed() {
        long total = 0;
        for (IndexingExecutionEngine<?, ?> engine : allEngines) {
            total += engine.getNativeBytesUsed();
        }
        return total;
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        for (IndexingExecutionEngine<?, ?> engine : allEngines) {
            engine.deleteFiles(filesToDelete);
        }
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        Map<DataFormat, DocumentInput<?>> inputMap = new HashMap<>();
        for (IndexingExecutionEngine<?, ?> engine : allEngines) {
            DocumentInput<?> input = engine.newDocumentInput();
            inputMap.put(engine.getDataFormat(), input);
        }
        return new CompositeDocumentInput(inputMap);
    }

    /**
     * Returns the primary indexing execution engine.
     *
     * @return the primary engine
     */
    public IndexingExecutionEngine<?, ?> getPrimaryEngine() {
        return primaryEngine;
    }

    /**
     * Returns an unmodifiable list of secondary indexing execution engines.
     *
     * @return the secondary engines
     */
    public List<IndexingExecutionEngine<?, ?>> getSecondaryEngines() {
        return secondaryEngines;
    }

    /**
     * Returns an unmodifiable list of all engines (primary first, then secondaries).
     *
     * @return all engines
     */
    public List<IndexingExecutionEngine<?, ?>> getAllEngines() {
        return allEngines;
    }
}
