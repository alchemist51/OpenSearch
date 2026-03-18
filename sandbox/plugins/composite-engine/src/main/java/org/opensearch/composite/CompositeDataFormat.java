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
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A composite {@link DataFormat} that wraps multiple per-format {@link DataFormat} instances.
 * Each constituent format retains its own {@link FieldTypeCapabilities} — field routing is
 * handled per-format by {@link CompositeDocumentInput}, not by this class.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDataFormat implements DataFormat {

    private final List<DataFormat> dataFormats;
    private final DataFormat primaryDataFormat;

    /**
     * Constructs a CompositeDataFormat from the given list of data formats.
     *
     * @param dataFormats the constituent data formats
     * @param primaryDataFormat the primary data format (must be contained in {@code dataFormats})
     */
    public CompositeDataFormat(List<DataFormat> dataFormats, DataFormat primaryDataFormat) {
        this.dataFormats = List.copyOf(Objects.requireNonNull(dataFormats, "dataFormats must not be null"));
        this.primaryDataFormat = Objects.requireNonNull(primaryDataFormat, "primaryDataFormat must not be null");
    }

    /**
     * Returns the primary data format.
     *
     * @return the primary data format
     */
    public DataFormat getPrimaryDataFormat() {
        return primaryDataFormat;
    }

    /**
     * Returns the list of constituent data formats.
     *
     * @return the data formats
     */
    public List<DataFormat> getDataFormats() {
        return dataFormats;
    }

    @Override
    public String name() {
        return "composite";
    }

    @Override
    public long priority() {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        return primaryDataFormat.supportedFields();
    }

    @Override
    public String toString() {
        return "CompositeDataFormat{" + "dataFormats=" + dataFormats + ", primaryDataFormat=" + primaryDataFormat + '}';
    }
}
