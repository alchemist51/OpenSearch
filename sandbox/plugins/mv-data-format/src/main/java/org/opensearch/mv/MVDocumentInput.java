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
 * No-op document input: the MV format never consumes per-document fields —
 * its data is derived from the primary's flushed parquet at flush time.
 */
public final class MVDocumentInput implements DocumentInput<Void> {

    @Override
    public Void getFinalInput() {
        return null;
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        // derived format: ignore all fields
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
