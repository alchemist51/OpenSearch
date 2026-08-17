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
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.Map;
import java.util.function.Supplier;

/**
 * POC(mv): registers the {@code mv_state} derived format for separate-index
 * MV TARGETS. A target opts in via
 * {@code index.composite.secondary_data_formats: [..., "mv_state"]}; the
 * format's writers maintain the FOLD of the shipped state (no ship targets of
 * their own — MV-over-MV chains would configure ship_targets here too).
 */
public class MVStateDataFormatPlugin extends Plugin implements DataFormatPlugin, SearchBackEndPlugin<MVReaderManager.MVReader> {

    public MVStateDataFormatPlugin() {}

    @Override
    public DataFormat getDataFormat() {
        return MVStateDataFormat.INSTANCE;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig config) {
        return new MVIndexingEngine(
            config.store().shardPath(),
            config.indexSettings().getIndex().getName(),
            MVDefinitionSpec.TARGET_FOLD,
            MVStateDataFormat.INSTANCE,
            java.util.List.of(),
            () -> null,
            () -> null
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        return Map.of(MVStateDataFormat.NAME, () -> new DataFormatDescriptor(MVStateDataFormat.NAME, new GenericCRC32ChecksumHandler()));
    }

    @Override
    public void assignCapabilities(MappedFieldType fieldType, IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        // Derived format: claims nothing (see MVDataFormatPlugin).
    }

    @Override
    public String name() {
        return MVStateDataFormat.NAME;
    }

    @Override
    public java.util.List<String> getSupportedFormats() {
        return java.util.List.of(MVStateDataFormat.NAME);
    }

    @Override
    public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
        return new MVReaderManager(MVStateDataFormat.NAME);
    }
}
