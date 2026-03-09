/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data.text;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParseContext;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Set;

public class IpLuceneField extends LuceneField {

    @Override
    public void createField(MappedFieldType fieldType, ParseContext.Document document, Object parseValue) {
        final InetAddress address = (InetAddress) parseValue;
        final byte[] encoded = InetAddresses.forString(address.getHostAddress()).getAddress();
        if (fieldType.isSearchable()) {
            document.add(new InetAddressPoint(fieldType.name(), InetAddresses.forString(address.getHostAddress())));
        }
        if (fieldType.hasDocValues()) {
            document.add(new SortedSetDocValuesField(fieldType.name(), new BytesRef(encoded)));
        }
        if (fieldType.isStored()) {
            document.add(new StoredField(fieldType.name(), new BytesRef(encoded)));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
