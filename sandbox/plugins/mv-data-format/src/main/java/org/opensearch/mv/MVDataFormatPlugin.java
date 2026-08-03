/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.Map;
import java.util.function.Supplier;

/**
 * POC(mv): the materialized-view data format plugin. Registers the derived
 * "materialized_view" format; indices opt in via
 * {@code index.composite.secondary_data_formats: ["materialized_view"]}.
 */
public class MVDataFormatPlugin extends Plugin implements DataFormatPlugin, SearchBackEndPlugin<MVReaderManager.MVReader> {

    public MVDataFormatPlugin() {}

    @Override
    public DataFormat getDataFormat() {
        return MVDataFormat.INSTANCE;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig config) {
        return new MVIndexingEngine(config.store().shardPath(), config.indexSettings().getIndex().getName());
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        return Map.of(MVDataFormat.NAME, () -> new DataFormatDescriptor(MVDataFormat.NAME, new GenericCRC32ChecksumHandler()));
    }

    @Override
    public void assignCapabilities(MappedFieldType fieldType, IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        // Derived format: claims no field capabilities ever. Setting an empty
        // map here would clobber assignments made by other formats — so do
        // nothing at all.
    }

    // ---- SearchBackEndPlugin (reader lifecycle for the materialized_view format) ----

    @Override
    public String name() {
        return MVDataFormat.NAME;
    }

    @Override
    public java.util.List<String> getSupportedFormats() {
        return java.util.List.of(MVDataFormat.NAME);
    }

    @Override
    public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
        return new MVReaderManager();
    }
}
