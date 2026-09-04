/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.mv;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class MVViewsServiceProviderTests extends OpenSearchTestCase {

    public void testViewsConfigurePullOnlySourceStorage() {
        Settings request = Settings.builder().putList(MVConstants.VIEWS_SETTING, "payments:payments_mv").build();

        Settings additional = new MVViewsService.Provider().getAdditionalIndexSettings("payments_source", false, request);

        assertTrue(additional.getAsBoolean("index.pluggable.dataformat.enabled", false));
        assertEquals("composite", additional.get("index.pluggable.dataformat"));
        assertEquals("parquet", additional.get("index.composite.primary_data_format"));
        assertEquals(List.of("lucene"), additional.getAsList("index.composite.secondary_data_formats"));
        assertFalse("pull-only source must not activate legacy ship targets", additional.hasValue(MVConstants.SHIP_TARGETS_SETTING));
        assertFalse("definition belongs to the derived target binding", additional.hasValue(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID));
    }

    public void testNoViewsContributesNoSettings() {
        assertSame(Settings.EMPTY, new MVViewsService.Provider().getAdditionalIndexSettings("plain", false, Settings.EMPTY));
    }
}
