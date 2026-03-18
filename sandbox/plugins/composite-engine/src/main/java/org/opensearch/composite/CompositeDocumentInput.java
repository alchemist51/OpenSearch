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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A composite {@link DocumentInput} that wraps one {@link DocumentInput} per registered
 * data format and routes field additions to the appropriate per-format inputs based on
 * {@link FieldTypeCapabilities}.
 * <p>
 * Metadata operations ({@code setRowId}, {@code setVersion}, {@code setSeqNo},
 * {@code setPrimaryTerm}) are broadcast to all per-format inputs. Fields are routed
 * only to formats whose {@link DataFormat#supportedFields()} includes a
 * {@link FieldTypeCapabilities} matching the field's type name.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeDocumentInput implements DocumentInput<Map<DataFormat, Object>> {

    private static final Logger logger = LogManager.getLogger(CompositeDocumentInput.class);

    private final Map<DataFormat, DocumentInput<?>> inputs;
    private final Map<String, Set<DataFormat>> fieldTypeToFormats;

    /**
     * Constructs a CompositeDocumentInput wrapping the given per-format inputs.
     * <p>
     * Precomputes a routing map from field type name to the set of data formats
     * that support that field type, based on each format's
     * {@link DataFormat#supportedFields()}.
     *
     * @param inputs a map of data format to its corresponding document input
     */
    public CompositeDocumentInput(Map<DataFormat, DocumentInput<?>> inputs) {
        this.inputs = Collections.unmodifiableMap(Objects.requireNonNull(inputs, "inputs must not be null"));
        this.fieldTypeToFormats = buildFieldTypeToFormatsMap(inputs);
    }

    private static Map<String, Set<DataFormat>> buildFieldTypeToFormatsMap(Map<DataFormat, DocumentInput<?>> inputs) {
        Map<String, Set<DataFormat>> routing = new HashMap<>();
        for (DataFormat format : inputs.keySet()) {
            for (FieldTypeCapabilities ftc : format.supportedFields()) {
                routing.computeIfAbsent(ftc.getFieldType(), k -> new HashSet<>()).add(format);
            }
        }
        return Collections.unmodifiableMap(routing);
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        String typeName = fieldType.typeName();
        Set<DataFormat> formats = fieldTypeToFormats.get(typeName);
        if (formats == null || formats.isEmpty()) {
            logger.debug("No data format supports field type [{}], field will be dropped", typeName);
            return;
        }
        for (DataFormat format : formats) {
            inputs.get(format).addField(fieldType, value);
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        for (DocumentInput<?> input : inputs.values()) {
            input.setRowId(rowIdFieldName, rowId);
        }
    }

    @Override
    public void setVersion(String fieldName, long version) {
        for (DocumentInput<?> input : inputs.values()) {
            input.setVersion(fieldName, version);
        }
    }

    @Override
    public void setSeqNo(String fieldName, long seqNo) {
        for (DocumentInput<?> input : inputs.values()) {
            input.setSeqNo(fieldName, seqNo);
        }
    }

    @Override
    public void setPrimaryTerm(String fieldName, long primaryTerm) {
        for (DocumentInput<?> input : inputs.values()) {
            input.setPrimaryTerm(fieldName, primaryTerm);
        }
    }

    @Override
    public Map<DataFormat, Object> getFinalInput() {
        Map<DataFormat, Object> result = new HashMap<>();
        for (Map.Entry<DataFormat, DocumentInput<?>> entry : inputs.entrySet()) {
            result.put(entry.getKey(), entry.getValue().getFinalInput());
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public void close() {
        for (DocumentInput<?> input : inputs.values()) {
            try {
                input.close();
            } catch (Exception e) {
                logger.warn("Failed to close per-format DocumentInput", e);
            }
        }
    }
}
