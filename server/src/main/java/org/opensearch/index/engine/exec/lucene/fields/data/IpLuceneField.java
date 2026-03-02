/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene.fields.data;

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
    public void createField(MappedFieldType mappedFieldType, ParseContext.Document document, Object parseValue, Set<FieldCapability> assignedCapabilities) {
        final InetAddress address = (InetAddress) parseValue;
        final byte[] encoded = InetAddresses.forString(address.getHostAddress()).getAddress();
        if (assignedCapabilities.contains(FieldCapability.INDEX)) {
            document.add(new InetAddressPoint(mappedFieldType.name(), InetAddresses.forString(address.getHostAddress())));
        }
        if (assignedCapabilities.contains(FieldCapability.DOC_VALUES)) {
            document.add(new SortedSetDocValuesField(mappedFieldType.name(), new BytesRef(encoded)));
        }
        if (assignedCapabilities.contains(FieldCapability.STORE)) {
            document.add(new StoredField(mappedFieldType.name(), new BytesRef(encoded)));
        }
    }

    @Override
    public Set<FieldCapability> getFieldCapabilities() {
        return EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
    }
}
