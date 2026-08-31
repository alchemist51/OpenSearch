/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.UUID;

public class DerivedIndexBindingTests extends OpenSearchTestCase {

    private static final String SOURCE_NAME = "clickbench";
    private static final String SOURCE_UUID = UUID.randomUUID().toString();
    private static final int SOURCE_SHARDS = 5;
    private static final int SOURCE_ROUTING_SHARDS = 5;
    private static final String DEFINITION_ID = "clickbench_q9";

    private static IndexMetadata fakeSourceMetadata(String name, String uuid, int shards, int routingShards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, uuid)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.number_of_routing_shards", routingShards)
            )
            .build();
    }

    private static IndexMetadata fakeSourceMetadata() {
        return fakeSourceMetadata(SOURCE_NAME, SOURCE_UUID, SOURCE_SHARDS, SOURCE_ROUTING_SHARDS);
    }

    public void testCreateFromMetadata() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        assertEquals(SOURCE_NAME, binding.sourceName());
        assertEquals(SOURCE_UUID, binding.sourceUuid());
        assertEquals(SOURCE_SHARDS, binding.sourceShardCount());
        assertEquals(SOURCE_ROUTING_SHARDS, binding.sourceRoutingShardCount());
        assertEquals(DerivedIndexBinding.MappingMode.IDENTITY, binding.mappingMode());
        assertEquals(DEFINITION_ID, binding.definitionId());
    }

    public void testRoundTripThroughSettings() {
        DerivedIndexBinding original = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        Settings serialized = original.toSettings();
        DerivedIndexBinding restored = DerivedIndexBinding.fromSettings(serialized);
        assertNotNull(restored);
        assertEquals(original, restored);
        assertEquals(original.hashCode(), restored.hashCode());
    }

    public void testFromSettingsReturnsNullWhenAbsent() {
        assertNull(DerivedIndexBinding.fromSettings(Settings.EMPTY));
    }

    public void testFromSettingsReturnsNullWhenUuidEmpty() {
        Settings settings = Settings.builder().put(DerivedIndexBinding.KEY_SOURCE_UUID, "").build();
        assertNull(DerivedIndexBinding.fromSettings(settings));
    }

    public void testFromSettingsWithMissingNameThrows() {
        Settings settings = Settings.builder()
            .put(DerivedIndexBinding.KEY_SOURCE_UUID, SOURCE_UUID)
            .put(DerivedIndexBinding.KEY_SOURCE_SHARDS, SOURCE_SHARDS)
            .build();
        expectThrows(IllegalStateException.class, () -> DerivedIndexBinding.fromSettings(settings));
    }

    public void testFromSettingsWithInvalidShardCountThrows() {
        Settings settings = Settings.builder()
            .put(DerivedIndexBinding.KEY_SOURCE_UUID, SOURCE_UUID)
            .put(DerivedIndexBinding.KEY_SOURCE_NAME, SOURCE_NAME)
            .put(DerivedIndexBinding.KEY_SOURCE_SHARDS, 0)
            .build();
        expectThrows(IllegalStateException.class, () -> DerivedIndexBinding.fromSettings(settings));
    }

    public void testNullDefinitionIdRoundTrips() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(fakeSourceMetadata(), DerivedIndexBinding.MappingMode.IDENTITY, null);
        Settings serialized = binding.toSettings();
        DerivedIndexBinding restored = DerivedIndexBinding.fromSettings(serialized);
        assertNotNull(restored);
        assertNull(restored.definitionId());
        assertEquals(binding, restored);
    }

    public void testPublicSettingsAreNotPrivate() {
        IndexScopedSettings iss = new IndexScopedSettings(Settings.EMPTY, new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS));
        assertFalse("KEY_SOURCE_NAME must NOT be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_SOURCE_NAME));
        assertFalse("KEY_DEFINITION_ID must NOT be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_DEFINITION_ID));
    }

    public void testPrivateSettingsArePrivate() {
        IndexScopedSettings iss = new IndexScopedSettings(Settings.EMPTY, new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS));
        assertTrue("KEY_SOURCE_UUID must be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_SOURCE_UUID));
        assertTrue("KEY_SOURCE_SHARDS must be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_SOURCE_SHARDS));
        assertTrue("KEY_SOURCE_ROUTING_SHARDS must be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_SOURCE_ROUTING_SHARDS));
        assertTrue("KEY_MAPPING_MODE must be private", iss.isPrivateSetting(DerivedIndexBinding.KEY_MAPPING_MODE));
    }

    public void testToPrivateSettingsOmitsPublicKeys() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        Settings priv = binding.toPrivateSettings();
        assertNotNull(priv.get(DerivedIndexBinding.KEY_SOURCE_UUID));
        assertNotNull(priv.get(DerivedIndexBinding.KEY_SOURCE_SHARDS));
        assertNotNull(priv.get(DerivedIndexBinding.KEY_SOURCE_ROUTING_SHARDS));
        assertNotNull(priv.get(DerivedIndexBinding.KEY_MAPPING_MODE));
        assertNull("toPrivateSettings must not contain source name", priv.get(DerivedIndexBinding.KEY_SOURCE_NAME));
        assertNull("toPrivateSettings must not contain definition id", priv.get(DerivedIndexBinding.KEY_DEFINITION_ID));
    }

    public void testHasDerivedSourceDeclaration() {
        assertTrue(
            DerivedIndexBinding.hasDerivedSourceDeclaration(
                Settings.builder().put(DerivedIndexBinding.KEY_SOURCE_NAME, SOURCE_NAME).build()
            )
        );
        assertFalse(DerivedIndexBinding.hasDerivedSourceDeclaration(Settings.EMPTY));
        assertFalse(
            DerivedIndexBinding.hasDerivedSourceDeclaration(Settings.builder().put(DerivedIndexBinding.KEY_SOURCE_NAME, "").build())
        );
    }

    public void testSettingsKeysAreUnderDerivedPrefix() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        for (String key : binding.toSettings().keySet()) {
            assertTrue("Setting key must start with index.derived.: " + key, key.startsWith("index.derived."));
        }
    }

    public void testSettingsNeverReuseResizeKeys() {
        Settings settings = DerivedIndexBinding.create(fakeSourceMetadata(), DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID)
            .toSettings();
        assertNull(settings.get(IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY));
        assertNull(settings.get(IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY));
    }

    public void testSourceIndex() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        Index sourceIndex = binding.sourceIndex();
        assertEquals(SOURCE_NAME, sourceIndex.getName());
        assertEquals(SOURCE_UUID, sourceIndex.getUUID());
    }

    public void testResolveSourceShardIdentity() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        for (int i = 0; i < SOURCE_SHARDS; i++) {
            assertEquals(i, binding.resolveSourceShard(i));
        }
    }

    public void testResolveSourceShardOutOfRangeThrows() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        expectThrows(IllegalArgumentException.class, () -> binding.resolveSourceShard(-1));
        expectThrows(IllegalArgumentException.class, () -> binding.resolveSourceShard(SOURCE_SHARDS));
    }

    public void testValidateTargetTopologyMatches() {
        DerivedIndexBinding.create(fakeSourceMetadata(), DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID)
            .validateTargetTopology(SOURCE_SHARDS);
    }

    public void testValidateTargetTopologyMismatchThrows() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        expectThrows(IllegalStateException.class, () -> binding.validateTargetTopology(SOURCE_SHARDS + 1));
        expectThrows(IllegalStateException.class, () -> binding.validateTargetTopology(1));
    }

    public void testValidateLiveSuccess() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        assertTrue(binding.validateLive(fakeSourceMetadata()).isValid());
    }

    public void testValidateLiveMissingSource() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        DerivedIndexBinding.ValidationResult r = binding.validateLive(null);
        assertFalse(r.isValid());
        assertTrue(r.reason().contains("does not exist"));
    }

    public void testValidateLiveUuidMismatch() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        IndexMetadata recreated = fakeSourceMetadata(SOURCE_NAME, UUID.randomUUID().toString(), SOURCE_SHARDS, SOURCE_ROUTING_SHARDS);
        DerivedIndexBinding.ValidationResult r = binding.validateLive(recreated);
        assertFalse(r.isValid());
        assertTrue(r.reason().contains("UUID mismatch"));
    }

    public void testValidateLiveShardCountChanged() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        IndexMetadata resized = fakeSourceMetadata(SOURCE_NAME, SOURCE_UUID, SOURCE_SHARDS + 2, SOURCE_ROUTING_SHARDS + 2);
        DerivedIndexBinding.ValidationResult r = binding.validateLive(resized);
        assertFalse(r.isValid());
        assertTrue(r.reason().contains("shard count changed"));
    }

    public void testCreateWithNullSourceThrows() {
        expectThrows(
            NullPointerException.class,
            () -> DerivedIndexBinding.create(null, DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID)
        );
    }

    public void testSingleShardBindingWorks() {
        IndexMetadata single = fakeSourceMetadata(SOURCE_NAME, SOURCE_UUID, 1, 1);
        DerivedIndexBinding binding = DerivedIndexBinding.create(single, DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID);
        assertEquals(1, binding.sourceShardCount());
        assertEquals(0, binding.resolveSourceShard(0));
        binding.validateTargetTopology(1);
    }

    public void testIndexMetadataAccessorReturnsBinding() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        IndexMetadata target = IndexMetadata.builder("mv_target")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SOURCE_SHARDS)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(binding.toSettings())
            )
            .build();
        assertEquals(binding, target.getDerivedIndexBinding());
    }

    public void testIndexMetadataAccessorReturnsNullForNonDerived() {
        IndexMetadata normalIndex = IndexMetadata.builder("regular_index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
        assertNull(normalIndex.getDerivedIndexBinding());
    }

    public void testEquality() {
        IndexMetadata md = fakeSourceMetadata();
        DerivedIndexBinding a = DerivedIndexBinding.create(md, DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID);
        DerivedIndexBinding b = DerivedIndexBinding.create(md, DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testInequalityDifferentUuid() {
        DerivedIndexBinding a = DerivedIndexBinding.create(fakeSourceMetadata(), DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID);
        DerivedIndexBinding b = DerivedIndexBinding.create(
            fakeSourceMetadata(SOURCE_NAME, UUID.randomUUID().toString(), SOURCE_SHARDS, SOURCE_ROUTING_SHARDS),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        assertNotEquals(a, b);
    }

    public void testToStringContainsKeyFields() {
        String str = DerivedIndexBinding.create(fakeSourceMetadata(), DerivedIndexBinding.MappingMode.IDENTITY, DEFINITION_ID).toString();
        assertTrue(str.contains(SOURCE_NAME));
        assertTrue(str.contains("IDENTITY"));
        assertTrue(str.contains(DEFINITION_ID));
    }

    public void testValidateLiveRoutingShardCountChanged() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        IndexMetadata changed = fakeSourceMetadata(SOURCE_NAME, SOURCE_UUID, SOURCE_SHARDS, SOURCE_ROUTING_SHARDS + 10);
        assertTrue("routing shard change alone should not fail validation", binding.validateLive(changed).isValid());
    }

    public void testIndexMetadataRoundTripPreservesBinding() {
        DerivedIndexBinding binding = DerivedIndexBinding.create(
            fakeSourceMetadata(),
            DerivedIndexBinding.MappingMode.IDENTITY,
            DEFINITION_ID
        );
        IndexMetadata target = IndexMetadata.builder("mv_target")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SOURCE_SHARDS)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(binding.toSettings())
            )
            .build();
        assertEquals(binding, IndexMetadata.builder(target).build().getDerivedIndexBinding());
    }

    public void testMappingModeFromStringCaseInsensitive() {
        assertEquals(DerivedIndexBinding.MappingMode.IDENTITY, DerivedIndexBinding.MappingMode.fromString("identity"));
        assertEquals(DerivedIndexBinding.MappingMode.IDENTITY, DerivedIndexBinding.MappingMode.fromString("IDENTITY"));
    }

    public void testMappingModeFromStringInvalidThrows() {
        expectThrows(IllegalArgumentException.class, () -> DerivedIndexBinding.MappingMode.fromString("fanout"));
    }
}
