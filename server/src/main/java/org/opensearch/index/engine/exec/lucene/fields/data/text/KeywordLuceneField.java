/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.FieldDescriptor;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class KeywordLuceneField extends LuceneField {

    @Override
    public void createField(FieldDescriptor descriptor, ParseContext.Document document, Object parseValue) {
        String value = (String) parseValue;
        final BytesRef binaryValue = new BytesRef(value);

        boolean shouldIndex = descriptor.isSearchable();
        boolean shouldStore = descriptor.isStored();

        if (shouldIndex || shouldStore) {
            FieldType fieldType = new FieldType();
            fieldType.setTokenized(false);
            fieldType.setStored(shouldStore);
            fieldType.setOmitNorms(true);
            fieldType.setIndexOptions(shouldIndex ? IndexOptions.DOCS : IndexOptions.NONE);
            fieldType.freeze();
            document.add(new KeywordFieldMapper.KeywordField(descriptor.fieldName(), binaryValue, fieldType));
        }

        if (descriptor.hasDocValues()) {
            document.add(new SortedSetDocValuesField(descriptor.fieldName(), binaryValue));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
