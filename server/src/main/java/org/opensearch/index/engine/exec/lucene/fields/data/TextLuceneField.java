/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.TextFieldMapper;

import java.util.Set;

public class TextLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, EngineRole engineRole, Set<FieldCapability> assignedCapabilities) {
        final TextFieldMapper.TextFieldType textFieldType = (TextFieldMapper.TextFieldType) mappedFieldType;
        final String value = (String) parseValue;
        FieldType fieldType = new FieldType();
        fieldType.setStored(textFieldType.isStored()); //TODO: What does it translate to?
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS); // TODO:: how to decide this one?
        Field field = new Field(textFieldType.name(), value, fieldType);
        document.add(field);
    }

    @Override
    public EngineRole getFieldRole() {
        return EngineRole.ALL;
    }
}
