/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields;

import org.opensearch.index.engine.exec.lucene.fields.data.DoubleLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.KeywordLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.LongLuceneField;
import org.opensearch.index.engine.exec.lucene.fields.data.TextLuceneField;
import org.opensearch.index.mapper.BooleanFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LuceneFieldRegistry {

    /**
     * All registered field mappings (thread-safe, mutable)
     */
    private static final Map<String, LuceneField> FIELD_REGISTRY = new ConcurrentHashMap<>();

    // Static initialization block to populate the field registry
    static {
        initialize();
    }

    // Private constructor to prevent instantiation of utility class
    private LuceneFieldRegistry() {
        throw new UnsupportedOperationException("Registry class should not be instantiated");
    }

    /**
     * Initialize the registry with all available plugins.
     * This method should be called during node startup after all plugins are loaded.
     */
    public static synchronized void initialize() {
        FIELD_REGISTRY.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.DOUBLE.typeName(), new DoubleLuceneField());
        FIELD_REGISTRY.put(NumberFieldMapper.NumberType.LONG.typeName(), new LongLuceneField());

        FIELD_REGISTRY.put(TextFieldMapper.CONTENT_TYPE, new TextLuceneField());

    }

    /**
     * Returns the LuceneField implementation for the specified OpenSearch field type, or null if not found.
     */
    public static LuceneField getLuceneField(String fieldType) {
        return FIELD_REGISTRY.get(fieldType);
    }

    /**
     * Returns all registered field type names.
     */
    public static java.util.Set<String> getRegisteredFieldNames() {
        return java.util.Collections.unmodifiableSet(FIELD_REGISTRY.keySet());
    }

}
