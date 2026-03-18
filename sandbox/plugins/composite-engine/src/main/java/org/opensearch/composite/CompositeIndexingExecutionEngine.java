/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;

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
public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, CompositeDocumentInput> {

    private final List<IndexingExecutionEngine<?, ?>> engines;
    private final IndexingExecutionEngine<?, ?> primaryEngine;
    private final CompositeDataFormat compositeDataFormat;

    /**
     * Constructs a CompositeIndexingExecutionEngine from the given per-format engines.
     * <p>
     * Identifies the primary engine by matching {@code getDataFormat().name()} to the
     * given {@code primaryFormatName}. Builds a {@link CompositeDataFormat} from the
     * union of all engines' supported field type capabilities.
     *
     * @param engines the list of per-format indexing execution engines
     * @param primaryFormatName the name of the primary data format
     * @throws IllegalArgumentException if no engine matches the primary format name
     */
    public CompositeIndexingExecutionEngine(List<IndexingExecutionEngine<?, ?>> engines, String primaryFormatName) {
        this.engines = List.copyOf(Objects.requireNonNull(engines, "engines must not be null"));
        Objects.requireNonNull(primaryFormatName, "primaryFormatName must not be null");

        IndexingExecutionEngine<?, ?> foundPrimary = null;
        for (IndexingExecutionEngine<?, ?> engine : this.engines) {
            if (engine.getDataFormat().name().equals(primaryFormatName)) {
                foundPrimary = engine;
                break;
            }
        }
        if (foundPrimary == null) {
            throw new IllegalArgumentException("No engine found matching primary format name [" + primaryFormatName + "]");
        }
        this.primaryEngine = foundPrimary;

        List<DataFormat> allFormats = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> engine : this.engines) {
            allFormats.add(engine.getDataFormat());
        }
        this.compositeDataFormat = new CompositeDataFormat(allFormats, foundPrimary.getDataFormat());
    }

    @Override
    public Writer<CompositeDocumentInput> createWriter(long writerGeneration) {
        Map<DataFormat, Writer<?>> writerMap = new LinkedHashMap<>();
        for (IndexingExecutionEngine<?, ?> engine : engines) {
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
        for (IndexingExecutionEngine<?, ?> engine : engines) {
            RefreshResult result = engine.refresh(refreshInput);
            allSegments.addAll(result.refreshedSegments());
        }
        return new RefreshResult(allSegments);
    }

    @Override
    public DataFormat getDataFormat() {
        return compositeDataFormat;
    }

    @Override
    public long getNativeBytesUsed() {
        long total = 0;
        for (IndexingExecutionEngine<?, ?> engine : engines) {
            total += engine.getNativeBytesUsed();
        }
        return total;
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        for (IndexingExecutionEngine<?, ?> engine : engines) {
            engine.deleteFiles(filesToDelete);
        }
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        Map<DataFormat, DocumentInput<?>> inputMap = new HashMap<>();
        for (IndexingExecutionEngine<?, ?> engine : engines) {
            DocumentInput<?> input = engine.newDocumentInput();
            inputMap.put(engine.getDataFormat(), input);
        }
        return new CompositeDocumentInput(inputMap);
    }
}
