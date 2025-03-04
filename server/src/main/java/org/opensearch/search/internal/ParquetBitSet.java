/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import java.util.BitSet;

public class ParquetBitSet {
    private BitSet bitset;
    private int size;

    public ParquetBitSet(int size) {
        bitset = new BitSet(size);
        this.size = size;
    }

    public void set(int index) {
       bitset.set(index);
    }

    public boolean get(long index) throws IndexOutOfBoundsException {
        if(index >= size) {
            throw new IndexOutOfBoundsException("Index " + index + " is out of bounds for size " + size);
        }
        return bitset.get((int)index);
    }

    public int size() {
        return size;
    }
}
