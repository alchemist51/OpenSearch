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

/**
 * Capturing document input (VSR model): the composite engine broadcasts every
 * field to every format; this input keeps only the MV's referenced column
 * ({@code service}) for the forward buffer.
 */
public final class MVDocumentInput implements DocumentInput<String> {

    private String service;

    @Override
    public String getFinalInput() {
        return service;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (MVConstants.GROUP_KEY.equals(fieldType.name()) && value != null) {
            this.service = value.toString();
        }
    }

    @Override
    public void setRowId(String rowIdFieldName, long rowId) {
        // derived format: row identity comes from the primary
    }

    @Override
    public long getFieldCount(String fieldName) {
        return 0;
    }

    @Override
    public void close() {}
}
