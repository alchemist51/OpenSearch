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
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.EnumSet;
import java.util.Set;

public class KeywordLuceneField extends LuceneField {

    private static final Logger logger = LogManager.getLogger(KeywordLuceneField.class);

    @Override
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, Set<FieldCapability> assignedCapabilities) {
        String value = (String) parseValue;
        final BytesRef binaryValue = new BytesRef(value);

        boolean shouldIndex = assignedCapabilities.contains(FieldCapability.INDEX);
        boolean shouldStore = assignedCapabilities.contains(FieldCapability.STORE);

        logger.info("[COMPOSITE_DEBUG] KeywordLuceneField.createField: field=[{}] value=[{}] capabilities={} shouldIndex={} shouldStore={} hasDocValues={}",
            mappedFieldType.name(), value, assignedCapabilities, shouldIndex, shouldStore, assignedCapabilities.contains(FieldCapability.DOC_VALUES));

        if (shouldIndex || shouldStore) {
            FieldType fieldType = new FieldType();
            fieldType.setTokenized(false);
            fieldType.setStored(shouldStore);
            fieldType.setOmitNorms(true);
            fieldType.setIndexOptions(shouldIndex ? IndexOptions.DOCS : IndexOptions.NONE);
            fieldType.freeze();
            document.add(new KeywordFieldMapper.KeywordField(mappedFieldType.name(), binaryValue, fieldType));
            logger.debug("[COMPOSITE_DEBUG] KeywordLuceneField: added KeywordField for [{}] indexed={} stored={}", mappedFieldType.name(), shouldIndex, shouldStore);
        }

        if (assignedCapabilities.contains(FieldCapability.DOC_VALUES)) {
            document.add(new SortedSetDocValuesField(mappedFieldType.name(), binaryValue));
            logger.debug("[COMPOSITE_DEBUG] KeywordLuceneField: added SortedSetDocValuesField for [{}]", mappedFieldType.name());
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
