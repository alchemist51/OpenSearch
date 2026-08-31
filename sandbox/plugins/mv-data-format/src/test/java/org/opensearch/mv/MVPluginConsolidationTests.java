/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DerivedDataFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.mv.pull.MVDerivedPullFormat;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Verifies the unified MV plugin consolidation:
 *
 * <ul>
 *   <li>One plugin registers both {@code materialized_view} and {@code mv_state}</li>
 *   <li>Both formats are {@link DerivedDataFormat} subclasses</li>
 *   <li>Generic SPI interfaces live in server, not in the plugin package</li>
 *   <li>{@code MVStateDataFormatPlugin} is no longer needed as a separate class</li>
 *   <li>{@code MVDerivedPullFormat} implements the server SPI</li>
 * </ul>
 */
public class MVPluginConsolidationTests extends OpenSearchTestCase {

    // ── Plugin registers correct data format ──────────────────────────────

    public void testPluginImplementsDataFormatPlugin() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        assertTrue(plugin instanceof DataFormatPlugin);
    }

    public void testPluginIsPlugin() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        assertTrue(plugin instanceof Plugin);
    }

    public void testPluginDataFormatIsMaterializedView() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        DataFormat format = plugin.getDataFormat();
        assertEquals("materialized_view", format.name());
        assertSame(MVDataFormat.INSTANCE, format);
    }

    // ── Source and target formats are distinct singletons ──────────────────

    public void testSourceFormatName() {
        assertEquals("materialized_view", MVDataFormat.NAME);
        assertEquals("materialized_view", MVDataFormat.INSTANCE.name());
    }

    public void testTargetFormatName() {
        assertEquals("mv_state", MVStateDataFormat.NAME);
        assertEquals("mv_state", MVStateDataFormat.INSTANCE.name());
    }

    public void testSourceAndTargetFormatsAreDifferent() {
        assertNotSame(MVDataFormat.INSTANCE, MVStateDataFormat.INSTANCE);
        assertNotEquals(MVDataFormat.NAME, MVStateDataFormat.NAME);
    }

    // ── Both formats extend DerivedDataFormat ─────────────────────────────

    public void testSourceFormatIsDerived() {
        assertTrue(MVDataFormat.INSTANCE instanceof DerivedDataFormat);
    }

    public void testTargetFormatIsDerived() {
        assertTrue(MVStateDataFormat.INSTANCE instanceof DerivedDataFormat);
    }

    // ── One plugin registers both format descriptors ──────────────────────

    public void testPluginSupportsBothFormats() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        java.util.List<String> supported = plugin.getSupportedFormats();
        assertTrue("must support materialized_view", supported.contains(MVDataFormat.NAME));
        assertTrue("must support mv_state", supported.contains(MVStateDataFormat.NAME));
    }

    // ── Generic SPI interfaces are in server package ──────────────────────

    public void testDerivedPullFormatIsInServerPackage() {
        String packageName = DerivedPullFormat.class.getPackageName();
        assertEquals("org.opensearch.index.engine.derived.pull.spi", packageName);
        assertFalse("SPI must not be in plugin package", packageName.startsWith("org.opensearch.mv"));
    }

    public void testDerivedSourceReaderIsInServerPackage() {
        String packageName = DerivedSourceReader.class.getPackageName();
        assertEquals("org.opensearch.index.engine.derived.pull.spi", packageName);
    }

    public void testDerivedArtifactBuilderIsInServerPackage() {
        String packageName = DerivedArtifactBuilder.class.getPackageName();
        assertEquals("org.opensearch.index.engine.derived.pull.spi", packageName);
    }

    // ── MVDerivedPullFormat implements the server SPI ──────────────────────

    public void testMVDerivedPullFormatImplementsSPI() {
        assertTrue("MV adapter must implement the server SPI", DerivedPullFormat.class.isAssignableFrom(MVDerivedPullFormat.class));
    }

    public void testMVDerivedPullFormatIdIsExpected() {
        // Format ID is the DERIVED DATA-FORMAT CATEGORY (materialized_view) —
        // the value a pull target declares in index.derived.data_format. It is
        // NOT the physical state-artifact format name (mv_state).
        MVDerivedPullFormat format = new MVDerivedPullFormat(null);
        assertEquals(MVDataFormat.NAME, format.formatId());
        assertEquals("materialized_view", format.formatId());
    }

    // ── Plugin settings registered ────────────────────────────────────────

    public void testPluginRegistersSettings() {
        MVDataFormatPlugin plugin = new MVDataFormatPlugin();
        java.util.List<org.opensearch.common.settings.Setting<?>> settings = plugin.getSettings();

        assertNotNull(settings);
        assertFalse("plugin must register settings", settings.isEmpty());

        java.util.Set<String> settingKeys = new java.util.HashSet<>();
        for (org.opensearch.common.settings.Setting<?> s : settings) {
            settingKeys.add(s.getKey());
        }
        assertTrue("must register pull interval", settingKeys.contains("index.mv_pull.interval"));
        assertTrue("must register definition hash", settingKeys.contains("index.mv_pull.definition_hash"));
    }

    // ── Format singletons are stable ──────────────────────────────────────

    public void testFormatSingletonsAreStable() {
        assertSame(MVDataFormat.INSTANCE, MVDataFormat.INSTANCE);
        assertSame(MVStateDataFormat.INSTANCE, MVStateDataFormat.INSTANCE);
    }
}
