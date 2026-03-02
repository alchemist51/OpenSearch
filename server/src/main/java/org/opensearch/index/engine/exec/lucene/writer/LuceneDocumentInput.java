/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.writer;

import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.engine.exec.lucene.fields.LuceneFieldRegistry;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.io.IOException;
import java.util.Set;

public class LuceneDocumentInput implements DocumentInput<ParseContext.Document> {
    private final ParseContext.Document document;
    private final IndexWriter indexWriter;
    private final EngineRole engineRole;
    private final FieldAssignments fieldAssignments;

    public LuceneDocumentInput(ParseContext.Document document, IndexWriter indexWriter, EngineRole engineRole, FieldAssignments fieldAssignments) {
        this.document = document;
        this.indexWriter = indexWriter;
        this.engineRole = engineRole;
        this.fieldAssignments = fieldAssignments;
    }

    @Override
    public void addRowIdField(String fieldName, long rowId) {
        document.add(new NumericDocValuesField(fieldName, rowId));
    }

    @SuppressWarnings("unchecked")
    @Override
    public void addField(MappedFieldType fieldType, Object value) {
        final String fieldTypeName = fieldType.typeName();

        // Check if this format should handle this field type at all
        if (!fieldAssignments.shouldHandle(fieldTypeName)) {
            return;
        }

        final LuceneField luceneField = LuceneFieldRegistry.getLuceneField(fieldTypeName);

        if (luceneField == null) {
            // Field type not supported by Lucene format — skip silently
            return;
        }

        Set<FieldCapability> assignedCapabilities = fieldAssignments.getAssignedCapabilities(fieldTypeName);
        luceneField.createField(fieldType, document, value, assignedCapabilities);
    }

    /**
     * Returns the underlying {@link ParseContext.Document} for ingesters to access
     * and add Lucene fields directly.
     */
    public ParseContext.Document getDocument() {
        return document;
    }

    @Override
    public EngineRole getEngineRole() {
        return engineRole;
    }

    @Override
    public ParseContext.Document getFinalInput() {
        return document;
    }

    @Override
    public WriteResult addToWriter() {
        try {
            long seqNum = indexWriter.addDocument(document);
            return new WriteResult(true, null, 1, 1, seqNum);
        } catch (IOException exception) {
            return new WriteResult(false, exception, 1, 1, 1);
        }
    }

    @Override
    public void close() throws Exception {
        // no-op, reuse writer
    }
}
