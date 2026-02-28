/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.ParseContext;

import java.util.Set;

public class DoubleLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, EngineRole engineRole, Set<FieldCapability> assignedCapabilities) {
        final NumberFieldMapper.NumberFieldType fieldType = (NumberFieldMapper.NumberFieldType) mappedFieldType;
        final Number value = (Number) parseValue;
        // Only storing the SortedNumericDocValuesField, assuming this is for the ROW_ID field
        document.add(new SortedNumericDocValuesField(fieldType.name(), NumericUtils.doubleToSortableLong(value.doubleValue())));
    }

    @Override
    public EngineRole getFieldRole() {
        return EngineRole.PRIMARY;
    }
}
