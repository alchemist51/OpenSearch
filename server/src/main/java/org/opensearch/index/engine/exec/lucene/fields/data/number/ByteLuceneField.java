/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.number;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.FieldDescriptor;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class ByteLuceneField extends LuceneField {

    @Override
    public void createField(FieldDescriptor descriptor, ParseContext.Document document, Object parseValue) {
        final Number value = (Number) parseValue;
        if (descriptor.isSearchable()) {
            document.add(new IntPoint(descriptor.fieldName(), value.byteValue()));
        }
        if (descriptor.hasDocValues()) {
            document.add(new SortedNumericDocValuesField(descriptor.fieldName(), value.byteValue()));
        }
        if (descriptor.isStored()) {
            document.add(new StoredField(descriptor.fieldName(), value.byteValue()));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
