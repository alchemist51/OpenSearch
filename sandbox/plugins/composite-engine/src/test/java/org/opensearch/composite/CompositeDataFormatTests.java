/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link CompositeDataFormat}.
 */
public class CompositeDataFormatTests extends OpenSearchTestCase {

    public void testNameReturnsComposite() {
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(List.of(primary), primary);
        assertEquals("composite", format.name());
    }

    public void testPriorityReturnsMaxValue() {
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of());
        CompositeDataFormat format = new CompositeDataFormat(List.of(primary), primary);
        assertEquals(Long.MAX_VALUE, format.priority());
    }

    public void testSupportedFieldsDelegatesToPrimaryFormat() {
        FieldTypeCapabilities primaryCap = new FieldTypeCapabilities("keyword", Set.of(FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH));
        FieldTypeCapabilities secondaryCap = new FieldTypeCapabilities(
            "integer",
            Set.of(FieldTypeCapabilities.Capability.COLUMNAR_STORAGE)
        );
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of(primaryCap));
        DataFormat secondary = createMockDataFormat("parquet", 2, Set.of(secondaryCap));

        CompositeDataFormat composite = new CompositeDataFormat(List.of(primary, secondary), primary);

        // supportedFields() should return only the primary's fields, not the union
        assertEquals(1, composite.supportedFields().size());
        assertTrue(composite.supportedFields().contains(primaryCap));
        assertFalse(composite.supportedFields().contains(secondaryCap));
    }

    public void testGetPrimaryDataFormat() {
        DataFormat lucene = createMockDataFormat("lucene", 1, Set.of());
        DataFormat parquet = createMockDataFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(List.of(lucene, parquet), lucene);
        assertSame(lucene, composite.getPrimaryDataFormat());
    }

    public void testGetDataFormats() {
        DataFormat lucene = createMockDataFormat("lucene", 1, Set.of());
        DataFormat parquet = createMockDataFormat("parquet", 2, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(List.of(lucene, parquet), lucene);
        assertEquals(2, composite.getDataFormats().size());
        assertSame(lucene, composite.getDataFormats().get(0));
        assertSame(parquet, composite.getDataFormats().get(1));
    }

    public void testGetDataFormatsIsUnmodifiable() {
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(List.of(primary), primary);
        expectThrows(UnsupportedOperationException.class, () -> composite.getDataFormats().add(createMockDataFormat("x", 0, Set.of())));
    }

    public void testConstructorRejectsNullDataFormats() {
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of());
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(null, primary));
    }

    public void testConstructorRejectsNullPrimaryDataFormat() {
        DataFormat format = createMockDataFormat("lucene", 1, Set.of());
        expectThrows(NullPointerException.class, () -> new CompositeDataFormat(List.of(format), null));
    }

    public void testToString() {
        DataFormat primary = createMockDataFormat("lucene", 1, Set.of());
        CompositeDataFormat composite = new CompositeDataFormat(List.of(primary), primary);
        String str = composite.toString();
        assertTrue(str.contains("CompositeDataFormat"));
        assertTrue(str.contains("dataFormats="));
        assertTrue(str.contains("primaryDataFormat="));
    }

    // Feature: composite-indexing-plugin, Property 2: supportedFields delegates to primary format
    // Validates: Requirements 2.4
    public void testPropertySupportedFieldsDelegatesToPrimary() {
        String[] fieldTypes = { "keyword", "text", "integer", "long", "float", "double", "date", "boolean", "geo_point", "binary" };
        FieldTypeCapabilities.Capability[] allCapabilities = FieldTypeCapabilities.Capability.values();

        for (int iteration = 0; iteration < 100; iteration++) {
            int numFormats = randomIntBetween(1, 5);
            List<DataFormat> formats = new ArrayList<>();

            for (int f = 0; f < numFormats; f++) {
                Set<FieldTypeCapabilities> formatCaps = new HashSet<>();
                int numCaps = randomIntBetween(0, 4);
                for (int c = 0; c < numCaps; c++) {
                    String fieldType = randomFrom(fieldTypes);
                    Set<FieldTypeCapabilities.Capability> caps = new HashSet<>();
                    int capCount = randomIntBetween(1, allCapabilities.length);
                    for (int k = 0; k < capCount; k++) {
                        caps.add(randomFrom(allCapabilities));
                    }
                    formatCaps.add(new FieldTypeCapabilities(fieldType, caps));
                }
                formats.add(createMockDataFormat("format-" + f, f, formatCaps));
            }

            DataFormat primary = formats.get(randomIntBetween(0, formats.size() - 1));
            CompositeDataFormat composite = new CompositeDataFormat(formats, primary);

            // supportedFields() must equal the primary's supportedFields()
            assertEquals(
                "supportedFields() must match primary format's fields [iteration=" + iteration + "]",
                primary.supportedFields(),
                composite.supportedFields()
            );
        }
    }

    private DataFormat createMockDataFormat(String name, long priority, Set<FieldTypeCapabilities> supportedFields) {
        return new DataFormat() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public long priority() {
                return priority;
            }

            @Override
            public Set<FieldTypeCapabilities> supportedFields() {
                return supportedFields;
            }
        };
    }
}
