/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * The single, shared entry point that turns a target index's settings into a
 * fully compiled {@link MVCompiledDefinition}. Every runtime consumer of an MV
 * definition (the pull artifact builder, the target-side state-merge strategy,
 * and — at creation — {@link MVViewsService}) resolves through here so the
 * resolution ORDER and the fail-closed integrity checks live in exactly one
 * place.
 *
 * <h2>Resolution order (Stage&nbsp;4)</h2>
 * <ol>
 *   <li><b>Persisted descriptor first.</b> If {@link MVConstants#DESCRIPTOR_SETTING}
 *       ({@code index.mv.descriptor}) is present, it is size-guarded, parsed via
 *       {@link MVDefinitionDescriptor#fromXContent} (fails closed on unparseable,
 *       unknown, or structurally-invalid JSON), then rebuilt via
 *       {@link MVCompiledDefinition#fromDescriptor} (fails closed on an embedded
 *       {@code definition_hash} that does not match the recomputed hash — i.e.
 *       tamper). A target created with the descriptor present is self-contained
 *       across restarts and never depends on the hardcoded
 *       {@link MVCompiledDefinition#compiledFor(String)} switch.</li>
 *   <li><b>Legacy named fallback.</b> Otherwise the definition name is taken
 *       from {@link MVConstants#DEFINITION_SETTING} (default {@code "payments"},
 *       preserving the prior {@code MVDerivedArtifactBuilder} behavior), or —
 *       when that is unset — {@link DerivedIndexBinding#KEY_DEFINITION_ID}, and
 *       compiled via {@link MVCompiledDefinition#compiledFor(String)}.</li>
 * </ol>
 *
 * <h2>Fail-closed contract</h2>
 * <p>Any tamper / oversize / unparseable / disagreeing descriptor throws an
 * unchecked exception from {@link #resolve(Settings)}. Because the pull path
 * calls this from the {@code MVDerivedArtifactBuilder} constructor — which the
 * {@code DerivedShardPoller} constructor invokes inside the
 * {@code NodeDerivedPullService} reconcile try/catch — a throw means the poller
 * is never registered and never starts. Definition identity is therefore
 * fail-closed across restarts.
 */
public final class MVDefinitionResolver {

    private MVDefinitionResolver() {}

    /**
     * Maximum serialized descriptor size, in bytes. The largest known named
     * definition ({@code heavy_l3}: 10 group keys + 120 aggregate descriptors)
     * serializes to roughly 8&nbsp;KB, so 64&nbsp;KB is ~8x headroom while still
     * being negligible next to the mappings the same {@code IndexMetadata}
     * already persists in cluster state. Descriptors above this cap are
     * rejected (fail closed) at both write and read.
     */
    public static final int MAX_DESCRIPTOR_BYTES = 64 * 1024;

    /**
     * The public, {@code Final}, {@code IndexScope} setting carrying the
     * persisted descriptor JSON. Registered by {@code MVDataFormatPlugin}.
     */
    public static final Setting<String> DESCRIPTOR_SETTING = Setting.simpleString(
        MVConstants.DESCRIPTOR_SETTING,
        "",
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    // ── Runtime resolution ────────────────────────────────────────────────

    /** Convenience overload over {@link IndexSettings}. */
    public static MVCompiledDefinition resolve(IndexSettings indexSettings) {
        return resolve(indexSettings.getSettings());
    }

    /**
     * Resolve the authoritative compiled definition for a target index.
     * Descriptor-first, fail-closed, with the legacy named fallback.
     *
     * @throws IllegalArgumentException if a persisted descriptor is present but
     *                                  oversize, unparseable, or fails its
     *                                  embedded integrity hash
     */
    public static MVCompiledDefinition resolve(Settings settings) {
        MVDefinitionDescriptor descriptor = descriptorFromSettings(settings);
        if (descriptor != null) {
            // fromDescriptor validates the embedded integrity hash and fails
            // closed on mismatch (tamper).
            return MVCompiledDefinition.fromDescriptor(descriptor);
        }
        return MVCompiledDefinition.compiledFor(legacyDefinitionName(settings));
    }

    /**
     * A short human-readable label for the resolved definition, for logging
     * only. Returns {@code "descriptor"} when a persisted descriptor is present,
     * else the legacy definition name.
     */
    public static String definitionLabel(Settings settings) {
        String json = settings.get(MVConstants.DESCRIPTOR_SETTING);
        if (json != null && json.isEmpty() == false) {
            return "descriptor";
        }
        return legacyDefinitionName(settings);
    }

    // ── Descriptor (de)serialization ──────────────────────────────────────

    /**
     * Parse the persisted descriptor from settings, or {@code null} when the
     * {@link MVConstants#DESCRIPTOR_SETTING} is absent or empty. Fails closed
     * (throws {@link IllegalArgumentException}) on an oversize or unparseable
     * descriptor.
     */
    public static MVDefinitionDescriptor descriptorFromSettings(Settings settings) {
        String json = settings.get(MVConstants.DESCRIPTOR_SETTING);
        if (json == null || json.isEmpty()) {
            return null;
        }
        int bytes = json.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "MV descriptor persisted in [%s] is %d bytes, exceeding the %d-byte limit",
                    MVConstants.DESCRIPTOR_SETTING,
                    bytes,
                    MAX_DESCRIPTOR_BYTES
                )
            );
        }
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            // fromXContent throws IllegalArgumentException on unknown/structural
            // tamper; that propagates directly (fail closed).
            return MVDefinitionDescriptor.fromXContent(parser);
        } catch (IOException e) {
            throw new IllegalArgumentException(
                "MV descriptor persisted in [" + MVConstants.DESCRIPTOR_SETTING + "] is not parseable JSON: " + e.getMessage(),
                e
            );
        }
    }

    /**
     * Serialize a descriptor to compact JSON suitable for persistence in the
     * {@link MVConstants#DESCRIPTOR_SETTING}. Fails closed if the result would
     * exceed {@link #MAX_DESCRIPTOR_BYTES}.
     */
    public static String serialize(MVDefinitionDescriptor descriptor) {
        final String json;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
            json = BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize MV definition descriptor", e);
        }
        int bytes = json.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_DESCRIPTOR_BYTES) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "serialized MV descriptor is %d bytes, exceeding the %d-byte limit for [%s]",
                    bytes,
                    MAX_DESCRIPTOR_BYTES,
                    MVConstants.DESCRIPTOR_SETTING
                )
            );
        }
        return json;
    }

    // ── Creation-time validation ──────────────────────────────────────────

    /**
     * Validate the definition settings of a create request, fail-closed. Called
     * by {@link MVViewsService} today and by the Stage&nbsp;5 REST create
     * endpoint next, so the agreement rule lives in one place.
     *
     * <p>Behavior:
     * <ul>
     *   <li>No descriptor present → no-op (legacy-only creation is allowed).</li>
     *   <li>Descriptor present → it is size-guarded, parsed, and its integrity
     *       hash validated (via {@link #descriptorFromSettings} +
     *       {@link MVCompiledDefinition#fromDescriptor}).</li>
     *   <li>Descriptor AND a legacy definition id both present → they must
     *       AGREE: {@code compiledFor(id).hash()} must equal the descriptor's
     *       recomputed {@code definition hash}. A mismatch rejects creation.</li>
     * </ul>
     *
     * @throws IllegalArgumentException on any oversize/unparseable/tampered
     *                                  descriptor, an uncompilable legacy id, or
     *                                  a descriptor/definition_id disagreement
     */
    public static void validateCreation(Settings settings) {
        MVDefinitionDescriptor descriptor = descriptorFromSettings(settings);
        if (descriptor == null) {
            return;
        }
        MVCompiledDefinition fromDescriptor = MVCompiledDefinition.fromDescriptor(descriptor);

        String legacyId = settings.get(MVConstants.DEFINITION_SETTING);
        if (legacyId == null || legacyId.isEmpty()) {
            legacyId = settings.get(DerivedIndexBinding.KEY_DEFINITION_ID);
        }
        if (legacyId == null || legacyId.isEmpty()) {
            return; // descriptor-only creation is self-contained and valid
        }

        final MVCompiledDefinition legacy;
        try {
            legacy = MVCompiledDefinition.compiledFor(legacyId);
        } catch (RuntimeException e) {
            throw new IllegalArgumentException(
                "cannot verify MV descriptor / definition_id agreement: legacy definition [" + legacyId + "] failed to compile: " + e
                    .getMessage(),
                e
            );
        }
        if (legacy.hash().equals(fromDescriptor.hash()) == false) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "MV descriptor and definition_id [%s] disagree: descriptor definition hash [%s] != compiled definition hash [%s]. "
                        + "A target may declare both only when they describe the same definition.",
                    legacyId,
                    fromDescriptor.hash(),
                    legacy.hash()
                )
            );
        }
    }

    // ── Internal ──────────────────────────────────────────────────────────

    /**
     * Extract the legacy definition name, preserving the prior
     * {@code MVDerivedArtifactBuilder} behavior:
     * {@code index.mv.definition} (default {@code "payments"}), falling back to
     * {@code index.derived.definition_id} only when the former is unset.
     */
    static String legacyDefinitionName(Settings settings) {
        String def = settings.get(MVConstants.DEFINITION_SETTING);
        if (def != null && def.isEmpty() == false) {
            return def;
        }
        String id = settings.get(DerivedIndexBinding.KEY_DEFINITION_ID);
        if (id != null && id.isEmpty() == false) {
            return id;
        }
        return "payments";
    }
}
