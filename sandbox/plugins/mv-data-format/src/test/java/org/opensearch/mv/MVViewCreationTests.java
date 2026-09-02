/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link MVViewCreation}: the create-path settings and mapping
 * must match the exact Stage&nbsp;4 derived-target contract and be identical to
 * the {@link MVViewsService.TargetCreator} auto-creation path.
 */
public class MVViewCreationTests extends OpenSearchTestCase {

    private static MVCompiledDefinition def() {
        return MVCompiledDefinition.compiledFor("clickbench_100m");
    }

    public void testBuildTargetSettingsMatchesStage4Contract() {
        MVCompiledDefinition def = def();
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        Settings s = MVViewCreation.buildTargetSettings("clickbench", 3, def, descriptorJson);

        assertEquals("clickbench", s.get(DerivedIndexBinding.KEY_SOURCE_NAME));
        assertEquals("3", s.get("index.number_of_shards"));
        assertEquals("0", s.get("index.number_of_replicas"));
        assertEquals("-1", s.get("index.refresh_interval"));
        assertEquals("true", s.get(MVConstants.DERIVED_INDEX_SETTING));
        assertEquals("true", s.get("index.append_only.enabled"));
        assertEquals("true", s.get("index.pluggable.dataformat.enabled"));
        assertEquals("composite", s.get("index.pluggable.dataformat"));
        assertEquals("parquet", s.get("index.composite.primary_data_format"));
        assertEquals(List.of("lucene"), s.getAsList("index.composite.secondary_data_formats"));
        assertEquals(MVDataFormat.NAME, s.get(DerivedIndexBinding.KEY_DATA_FORMAT));
        assertEquals("clickbench", s.get(MVConstants.COLOCATE_WITH_SETTING));
        assertEquals(descriptorJson, s.get(MVConstants.DESCRIPTOR_SETTING));
        // state_fields == compiled state column names (the durable Arrow<->mapping bridge)
        assertEquals(def.stateColumnNames(), s.getAsList(MVConstants.STATE_FIELDS_SETTING));

        // Descriptor-only creation: no legacy definition id/name is set.
        assertNull(s.get(DerivedIndexBinding.KEY_DEFINITION_ID));
        assertNull(s.get(MVConstants.DEFINITION_SETTING));

        // The exact contract gate the auto-creation path also runs must pass.
        MVDefinitionResolver.validateCreation(s);
    }

    public void testTargetMappingHasProvenanceDynamicFalseAndAllAliases() {
        MVCompiledDefinition def = def();
        String mapping = MVViewCreation.targetMapping(def);
        assertTrue(mapping.contains("\"dynamic\":\"false\""));
        assertTrue(mapping.contains("\"_field_names\":{\"enabled\":false}"));
        assertTrue(mapping.contains("\"_mv_source_generation\":{\"type\":\"long\",\"index\":false}"));
        // Every user-visible alias appears in the mapping.
        for (String field : def.targetMapping().keySet()) {
            assertTrue("mapping must contain field [" + field + "]", mapping.contains("\"" + field + "\":{"));
        }
    }

    public void testTargetMappingIdenticalToAutoCreationPath() {
        // The REST create path and the index.mv.views auto-creation path must
        // emit a byte-identical mapping for the same definition.
        String viaHelper = MVViewCreation.targetMapping(MVCompiledDefinition.compiledFor("clickbench_100m"));
        String viaTargetCreator = MVViewsService.TargetCreator.targetMapping("clickbench_100m");
        assertEquals(viaTargetCreator, viaHelper);
    }

    public void testDefaultTargetIndex() {
        assertEquals("clickbench_mv_q9", MVViewCreation.defaultTargetIndex("clickbench", "q9"));
    }
}
