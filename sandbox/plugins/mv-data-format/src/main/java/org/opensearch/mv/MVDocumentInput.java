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
 * Capturing document input (VSR model, v2): keeps the MV's referenced
 * columns — group keys (service, status) and the metric (latency_ms) —
 * from the composite broadcast.
 */
public final class MVDocumentInput implements DocumentInput<MVDocumentInput.Row> {

    /** Captured values for one document. */
    public record Row(String service, String status, Long latencyMs) {}

    private String service;
    private String status;
    private Long latencyMs;

    @Override
    public Row getFinalInput() {
        return new Row(service, status, latencyMs);
    }

    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        if (value == null) {
            return;
        }
        switch (fieldType.name()) {
            case "service" -> this.service = value.toString();
            case "status" -> this.status = value.toString();
            case "latency_ms" -> this.latencyMs = ((Number) value).longValue();
            default -> { /* not referenced by the MV */ }
        }
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
