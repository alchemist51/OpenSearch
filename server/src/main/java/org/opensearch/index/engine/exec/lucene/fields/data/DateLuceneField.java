/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class DateLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, Set<FieldCapability> assignedCapabilities) {
        final long timestamp = (long) parseValue;
        if (assignedCapabilities.contains(FieldCapability.INDEX)) {
            document.add(new LongPoint(mappedFieldType.name(), timestamp));
        }
        if (assignedCapabilities.contains(FieldCapability.DOC_VALUES)) {
            document.add(new SortedNumericDocValuesField(mappedFieldType.name(), timestamp));
        }
        if (assignedCapabilities.contains(FieldCapability.STORE)) {
            document.add(new StoredField(mappedFieldType.name(), timestamp));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
