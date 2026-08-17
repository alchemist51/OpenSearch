/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.List;

/**
 * Capturing document input (VSR model): keeps the definition's referenced
 * columns from the composite broadcast, in the spec's buffer order (group
 * keys first). Spec-driven so the same class serves the SOURCE definition
 * (raw fields) and the TARGET fold definition (state fields).
 */
public final class MVDocumentInput implements DocumentInput<Object[]> {

    private final List<MVDefinitionSpec.Column> columns;
    private final Object[] values;

    public MVDocumentInput(MVDefinitionSpec spec) {
        this.columns = spec.columns();
        this.values = new Object[columns.size()];
    }

    @Override
    public Object[] getFinalInput() {
        Object[] row = values.clone();
        java.util.Arrays.fill(values, null);
        return row;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (value == null) {
            return;
        }
        for (int i = 0; i < columns.size(); i++) {
            MVDefinitionSpec.Column col = columns.get(i);
            if (col.name().equals(fieldType.name())) {
                values[i] = col.type() == MVDefinitionSpec.ColumnType.UTF8 ? value.toString() : ((Number) value).longValue();
                return;
            }
        }
        // Field not referenced by the definition (e.g. provenance fields on
        // the target) — ignored.
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {}

    @Override
    public long getFieldCount(String fieldName) {
        return 0;
    }

    @Override
    public void close() {}
}
