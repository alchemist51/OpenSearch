/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class KeywordLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, Set<FieldCapability> assignedCapabilities) {
        String value = (String) parseValue;
        final BytesRef binaryValue = new BytesRef(value);

        boolean shouldIndex = assignedCapabilities.contains(FieldCapability.INDEX);
        boolean shouldStore = assignedCapabilities.contains(FieldCapability.STORE);

        if (shouldIndex || shouldStore) {
            FieldType fieldType = new FieldType();
            fieldType.setTokenized(false);
            fieldType.setStored(shouldStore);
            fieldType.setOmitNorms(true);
            fieldType.setIndexOptions(shouldIndex ? IndexOptions.DOCS : IndexOptions.NONE);
            fieldType.freeze();
            document.add(new KeywordFieldMapper.KeywordField(mappedFieldType.name(), binaryValue, fieldType));
        }

        if (assignedCapabilities.contains(FieldCapability.DOC_VALUES)) {
            document.add(new SortedSetDocValuesField(mappedFieldType.name(), binaryValue));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
