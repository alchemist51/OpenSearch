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
import org.opensearch.index.engine.exec.FieldDescriptor;
import org.opensearch.index.engine.exec.lucene.fields.LuceneField;
import org.opensearch.index.mapper.ParseContext;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Set;

public class IpLuceneField extends LuceneField {

    @Override
    public void createField(FieldDescriptor descriptor, ParseContext.Document document, Object parseValue) {
        final InetAddress address = (InetAddress) parseValue;
        final byte[] encoded = InetAddresses.forString(address.getHostAddress()).getAddress();
        if (descriptor.isSearchable()) {
            document.add(new InetAddressPoint(descriptor.fieldName(), InetAddresses.forString(address.getHostAddress())));
        }
        if (descriptor.hasDocValues()) {
            document.add(new SortedSetDocValuesField(descriptor.fieldName(), new BytesRef(encoded)));
        }
        if (descriptor.isStored()) {
            document.add(new StoredField(descriptor.fieldName(), new BytesRef(encoded)));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
