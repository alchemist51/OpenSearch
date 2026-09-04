/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;

/**
 * Stage 4 tests for {@link MVDefinitionResolver}: the single, shared,
 * fail-closed resolution of a target index's MV definition.
 *
 * <p>Covers the resolution order (persisted descriptor wins over the legacy
 * {@code definition_id}; legacy-only still works), a real
 * {@link IndexMetadata}/{@link Settings} persist → read → resolve round-trip,
 * the fail-closed behaviors (tamper / unparseable / unknown field / oversize),
 * the size guard at its boundary, and the creation-time agreement check.
 */
public class MVDefinitionResolverTests extends OpenSearchTestCase {

    // ── Resolution order ──────────────────────────────────────────────────

    public void testDescriptorWinsOverDefinitionId() {
        // Descriptor describes heavy_l1; the legacy id points at a DIFFERENT
        // definition. At runtime the descriptor is authoritative.
        MVCompiledDefinition descriptorDef = MVCompiledDefinition.compiledFor("heavy_l1");
        String json = MVDefinitionResolver.serialize(descriptorDef.toDescriptor());

        Settings settings = Settings.builder()
            .put(MVConstants.DESCRIPTOR_SETTING, json)
            .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "clickbench_100m")
            .build();

        MVCompiledDefinition resolved = MVDefinitionResolver.resolve(settings);
        assertEquals(descriptorDef.hash(), resolved.hash());
        assertNotEquals(MVCompiledDefinition.compiledFor("clickbench_100m").hash(), resolved.hash());
    }

    public void testLegacyOnlyStillWorks() {
        Settings settings = Settings.builder().put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "clickbench_100m").build();
        MVCompiledDefinition resolved = MVDefinitionResolver.resolve(settings);
        assertEquals(MVCompiledDefinition.compiledFor("clickbench_100m").hash(), resolved.hash());
    }

    public void testResolveFailsClosedWhenNoDefinitionDeclared() {
        // The silent "payments" default was removed: no descriptor and no
        // index.derived.definition_id means resolution must fail loudly.
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.resolve(Settings.EMPTY));
    }

    public void testDefinitionIdFallbackWhenNoDefinitionSetting() {
        Settings settings = Settings.builder().put(DerivedIndexBinding.KEY_DEFINITION_ID, "heavy_l1").build();
        MVCompiledDefinition resolved = MVDefinitionResolver.resolve(settings);
        assertEquals(MVCompiledDefinition.compiledFor("heavy_l1").hash(), resolved.hash());
    }

    public void testDefinitionLabel() {
        assertEquals("clickbench_100m", MVDefinitionResolver.definitionLabel(Settings.builder().put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "clickbench_100m").build()));
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        assertEquals("descriptor", MVDefinitionResolver.definitionLabel(Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, json).build()));
    }

    // ── Real IndexMetadata / Settings round-trip ─────────────────────────

    public void testRoundTripThroughIndexMetadata() {
        for (String name : new String[] { "payments", "clickbench_5m_url", "heavy_l3" }) {
            MVCompiledDefinition original = MVCompiledDefinition.compiledFor(name);
            String json = MVDefinitionResolver.serialize(original.toDescriptor());

            IndexMetadata imd = IndexMetadata.builder("mv_target_" + name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DerivedIndexBinding.KEY_DEFINITION_ID, name)
                        .put(MVConstants.DESCRIPTOR_SETTING, json)
                )
                .build();

            Settings readBack = imd.getSettings();
            MVCompiledDefinition resolved = MVDefinitionResolver.resolve(readBack);
            assertEquals(name + ": definition hash survives IndexMetadata round-trip", original.hash(), resolved.hash());
            assertEquals(name + ": projection order survives", original.stateColumnNames(), resolved.stateColumnNames());
        }
    }

    public void testSerializeReadRoundTripEqualsDescriptor() {
        MVDefinitionDescriptor original = MVCompiledDefinition.compiledFor("heavy_l2").toDescriptor();
        String json = MVDefinitionResolver.serialize(original);
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, json).build();
        MVDefinitionDescriptor parsed = MVDefinitionResolver.descriptorFromSettings(settings);
        assertEquals(original, parsed);
    }

    public void testDescriptorFromSettingsAbsentIsNull() {
        assertNull(MVDefinitionResolver.descriptorFromSettings(Settings.EMPTY));
        assertNull(MVDefinitionResolver.descriptorFromSettings(Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, "").build()));
    }

    // ── Fail-closed: tamper / unparseable / unknown field ────────────────

    public void testTamperedIntegrityHashRejected() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        // Corrupt the embedded integrity hash while keeping the JSON structural.
        String tampered = json.replaceFirst("\"definition_hash\":\"[0-9a-f]+\"", "\"definition_hash\":\"deadbeefdeadbeef\"");
        assertNotEquals("expected the tamper to change the JSON", json, tampered);

        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, tampered).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.resolve(settings));
        assertThat(e.getMessage(), containsString("integrity"));
    }

    public void testUnparseableDescriptorRejected() {
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, "{ \"descriptor_version\": 1, ").build();
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.resolve(settings));
    }

    public void testUnknownFieldRejected() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        // Inject an unknown top-level field — fromXContent fails closed.
        String withUnknown = json.replaceFirst("\\{", "{\"bogus_field\":123,");
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, withUnknown).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.resolve(settings));
        assertThat(e.getMessage(), containsString("bogus_field"));
    }

    // ── Fail-closed: oversize guard at the boundary ──────────────────────

    public void testOversizeRawSettingRejected() {
        String tooBig = "x".repeat(MVDefinitionResolver.MAX_DESCRIPTOR_BYTES + 1);
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, tooBig).build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.descriptorFromSettings(settings));
        assertThat(e.getMessage(), containsString("exceeding"));
    }

    public void testSerializeRejectsOversizeDescriptor() {
        // Pad provenance sourceText so the serialized descriptor crosses the cap.
        String pad = "x".repeat(MVDefinitionResolver.MAX_DESCRIPTOR_BYTES + 100);
        MVDefinitionDescriptor big = MVDefinitionDescriptor.fromCompiled(MVCompiledDefinition.compiledFor("heavy_l1"), "ppl", pad);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.serialize(big));
        assertThat(e.getMessage(), containsString("exceeding"));
    }

    public void testSerializeAcceptsDescriptorJustUnderBoundary() {
        // A padded-but-under-limit descriptor serializes and round-trips cleanly.
        String pad = "x".repeat(1024);
        MVDefinitionDescriptor ok = MVDefinitionDescriptor.fromCompiled(MVCompiledDefinition.compiledFor("heavy_l1"), "ppl", pad);
        String json = MVDefinitionResolver.serialize(ok);
        assertTrue(json.getBytes(java.nio.charset.StandardCharsets.UTF_8).length <= MVDefinitionResolver.MAX_DESCRIPTOR_BYTES);
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, json).build();
        // Integrity holds (fromCompiled embedded the real hash) → resolve succeeds.
        assertEquals(ok, MVDefinitionResolver.descriptorFromSettings(settings));
        assertEquals(MVCompiledDefinition.compiledFor("heavy_l1").hash(), MVDefinitionResolver.resolve(settings).hash());
    }

    // ── Creation-time agreement check ─────────────────────────────────────

    public void testValidateCreationRejectsDisagreement() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        Settings settings = Settings.builder()
            .put(MVConstants.DESCRIPTOR_SETTING, json)
            .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "clickbench_100m")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.validateCreation(settings));
        assertThat(e.getMessage(), containsString("disagree"));
    }

    public void testValidateCreationRejectsDisagreementViaDefinitionId() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        Settings settings = Settings.builder()
            .put(MVConstants.DESCRIPTOR_SETTING, json)
            .put(DerivedIndexBinding.KEY_DEFINITION_ID, "clickbench_100m")
            .build();
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.validateCreation(settings));
    }

    public void testValidateCreationAcceptsAgreement() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        Settings settings = Settings.builder()
            .put(MVConstants.DESCRIPTOR_SETTING, json)
            .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "heavy_l1")
            .build();
        MVDefinitionResolver.validateCreation(settings); // must not throw
    }

    public void testValidateCreationDescriptorOnlyIsValid() {
        String json = MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("heavy_l1").toDescriptor());
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, json).build();
        MVDefinitionResolver.validateCreation(settings); // no legacy id → self-contained, no throw
    }

    public void testValidateCreationLegacyOnlyIsNoop() {
        Settings settings = Settings.builder().put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, "heavy_l1").build();
        MVDefinitionResolver.validateCreation(settings); // no descriptor → no-op
    }

    public void testValidateCreationRejectsOversize() {
        String tooBig = "x".repeat(MVDefinitionResolver.MAX_DESCRIPTOR_BYTES + 1);
        Settings settings = Settings.builder().put(MVConstants.DESCRIPTOR_SETTING, tooBig).build();
        expectThrows(IllegalArgumentException.class, () -> MVDefinitionResolver.validateCreation(settings));
    }

    /**
     * Mirrors exactly what {@code MVViewsService.TargetCreator.createTarget}
     * persists — a descriptor derived from {@code compiledFor(definition)}
     * alongside {@code definition_id == definition}. For every registered named
     * definition the pair must agree and round-trip back to the same hash, so
     * the write path can never produce a self-contradictory target.
     */
    public void testViewsServiceWritePathAgreesAndRoundTripsForAllNames() {
        for (String name : MVDefinitionSpec.allNames()) {
            MVCompiledDefinition compiled = MVCompiledDefinition.compiledFor(name);
            String json = MVDefinitionResolver.serialize(compiled.toDescriptor());
            Settings settings = Settings.builder()
                .put(MVConstants.DESCRIPTOR_SETTING, json)
                .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, name)
                .put(DerivedIndexBinding.KEY_DEFINITION_ID, name)
                .build();
            MVDefinitionResolver.validateCreation(settings); // must not throw
            assertEquals(name, compiled.hash(), MVDefinitionResolver.resolve(settings).hash());
        }
    }
}
