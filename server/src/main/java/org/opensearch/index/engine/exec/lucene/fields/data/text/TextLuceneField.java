/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.FieldDescriptor;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.ParseContext;

import java.util.EnumSet;
import java.util.Set;

public class TextLuceneField extends LuceneField {

    @Override
    public void createField(FieldDescriptor descriptor, ParseContext.Document document, Object parseValue) {
        final String value = (String) parseValue;

        boolean shouldIndex = descriptor.isSearchable();
        boolean shouldStore = descriptor.isStored();

        if (shouldIndex || shouldStore) {
            FieldType fieldType = new FieldType();
            fieldType.setStored(shouldStore);
            fieldType.setIndexOptions(shouldIndex ? IndexOptions.DOCS_AND_FREQS_AND_POSITIONS : IndexOptions.NONE);
            Field field = new Field(descriptor.fieldName(), value, fieldType);
            document.add(field);
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX);
    }
}
