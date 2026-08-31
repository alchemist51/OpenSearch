/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;

import java.util.Locale;
import java.util.Objects;

/**
 * Durable, immutable identity binding from a derived index (MV target, etc.)
 * to its source index. Persisted in the target {@link IndexMetadata} as a
 * mix of public and private {@code index.derived.*} settings.
 *
 * <h2>Public vs Private contract</h2>
 * <ul>
 *   <li><b>Public (user-provided at create time)</b>:
 *       {@link #SETTING_SOURCE_NAME} and {@link #SETTING_DEFINITION_ID}.
 *       These are the user's intent — "derive from this source, using
 *       this definition." They are registered as {@code IndexScope, Final}
 *       settings in {@link org.opensearch.common.settings.IndexScopedSettings}.</li>
 *   <li><b>Private (server-generated at create time)</b>:
 *       {@link #KEY_SOURCE_UUID}, {@link #KEY_SOURCE_SHARDS},
 *       {@link #KEY_SOURCE_ROUTING_SHARDS}, {@link #KEY_MAPPING_MODE}.
 *       Injected by {@link MetadataCreateIndexService} after resolving the
 *       source from cluster state. Users cannot submit or spoof these via
 *       REST — they are rejected by the private-setting validation gate.</li>
 * </ul>
 *
 * <p><b>v1 contract</b>: identity-only mapping. Target shard N reads source
 * shard N. No shrink/fan-in, split/fan-out, data streams, or automatic
 * rebind. Source delete-and-recreate (same name, new UUID) or topology
 * mismatch (shard count change) fails closed — the pull path refuses to
 * advance the watermark and requires a full rebuild.
 *
 * <p>The source index is never made aware of this binding. No cluster-state
 * watermark churn is introduced; mutable watermarks remain in shard commit
 * user data / catalog metadata.
 *
 * <p>Resize of a bound derived target is rejected: the identity topology
 * is immutable and any in-place change would violate the N:N shard mapping.
 *
 * @opensearch.experimental
 */
@org.opensearch.common.annotation.ExperimentalApi
public final class DerivedIndexBinding {

    // ── Public setting keys (user-provided creation input) ───────────────
    // Registered in IndexScopedSettings.BUILT_IN_INDEX_SETTINGS as
    // IndexScope + Final. Users supply these in the create-index request.

    /**
     * Source index name — the user's intent. Presence triggers the
     * derived-index enrichment path in MetadataCreateIndexService.
     */
    public static final String KEY_SOURCE_NAME = "index.derived.source.name";

    /** Public Setting object for source name (IndexScope, Final). */
    public static final Setting<String> SETTING_SOURCE_NAME = Setting.simpleString(
        KEY_SOURCE_NAME,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    /**
     * Definition identity (definition name / hash) — user-visible,
     * optionally provided at creation time.
     */
    public static final String KEY_DEFINITION_ID = "index.derived.definition_id";

    /** Public Setting object for definition ID (IndexScope, Final). */
    public static final Setting<String> SETTING_DEFINITION_ID = Setting.simpleString(
        KEY_DEFINITION_ID,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    // ── Private setting keys (server-generated, never user-settable) ─────
    // Recognized by IndexScopedSettings.isPrivateSetting(). Injected by
    // MetadataCreateIndexService.enrichDerivedIndexSettings() at creation.

    /** Source index UUID at binding time — the identity anchor. */
    public static final String KEY_SOURCE_UUID = "index.derived.source.uuid";

    /** Source number_of_shards at binding time. */
    public static final String KEY_SOURCE_SHARDS = "index.derived.source.number_of_shards";

    /** Source number_of_routing_shards at binding time. */
    public static final String KEY_SOURCE_ROUTING_SHARDS = "index.derived.source.number_of_routing_shards";

    /** Shard mapping mode (v1: IDENTITY only). */
    public static final String KEY_MAPPING_MODE = "index.derived.mapping_mode";

    // ── Mapping modes ────────────────────────────────────────────────────

    /** Shard mapping modes. v1 only implements IDENTITY. */
    @org.opensearch.common.annotation.ExperimentalApi
    public enum MappingMode {
        /**
         * Target shard N reads source shard N.
         * Requires target shard count == source shard count.
         */
        IDENTITY;

        public static MappingMode fromString(String s) {
            return MappingMode.valueOf(s.toUpperCase(Locale.ROOT));
        }
    }

    // ── Instance fields ──────────────────────────────────────────────────

    private final String sourceName;
    private final String sourceUuid;
    private final int sourceShardCount;
    private final int sourceRoutingShardCount;
    private final MappingMode mappingMode;
    @Nullable
    private final String definitionId;

    private DerivedIndexBinding(
        String sourceName,
        String sourceUuid,
        int sourceShardCount,
        int sourceRoutingShardCount,
        MappingMode mappingMode,
        @Nullable String definitionId
    ) {
        this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
        this.sourceUuid = Objects.requireNonNull(sourceUuid, "sourceUuid");
        if (sourceShardCount < 1) {
            throw new IllegalArgumentException("sourceShardCount must be >= 1, got " + sourceShardCount);
        }
        if (sourceRoutingShardCount < sourceShardCount) {
            throw new IllegalArgumentException(
                "sourceRoutingShardCount [" + sourceRoutingShardCount + "] < sourceShardCount [" + sourceShardCount + "]"
            );
        }
        this.sourceShardCount = sourceShardCount;
        this.sourceRoutingShardCount = sourceRoutingShardCount;
        this.mappingMode = Objects.requireNonNull(mappingMode, "mappingMode");
        this.definitionId = definitionId;
    }

    // ── Factory methods ──────────────────────────────────────────────────

    /**
     * Create a binding by capturing source identity from live cluster state.
     *
     * @param sourceMetadata the source {@link IndexMetadata} — must be non-null
     * @param mode           shard mapping mode
     * @param definitionId   optional definition name/hash
     * @return immutable binding
     */
    public static DerivedIndexBinding create(IndexMetadata sourceMetadata, MappingMode mode, @Nullable String definitionId) {
        Objects.requireNonNull(sourceMetadata, "sourceMetadata");
        return new DerivedIndexBinding(
            sourceMetadata.getIndex().getName(),
            sourceMetadata.getIndexUUID(),
            sourceMetadata.getNumberOfShards(),
            sourceMetadata.getRoutingNumShards(),
            mode,
            definitionId
        );
    }

    /**
     * Deserialize a binding from target index settings. Returns {@code null}
     * if no binding is present (the index is not a derived index, or the
     * binding was never set).
     */
    public static DerivedIndexBinding fromSettings(Settings settings) {
        String uuid = settings.get(KEY_SOURCE_UUID);
        if (uuid == null || uuid.isEmpty()) {
            return null;
        }
        String name = settings.get(KEY_SOURCE_NAME);
        if (name == null || name.isEmpty()) {
            throw new IllegalStateException("derived index binding has source UUID [" + uuid + "] but no source name");
        }
        int shards = settings.getAsInt(KEY_SOURCE_SHARDS, -1);
        if (shards < 1) {
            throw new IllegalStateException("derived index binding has invalid source shard count [" + shards + "]");
        }
        int routingShards = settings.getAsInt(KEY_SOURCE_ROUTING_SHARDS, shards);
        String modeStr = settings.get(KEY_MAPPING_MODE, MappingMode.IDENTITY.name());
        return new DerivedIndexBinding(name, uuid, shards, routingShards, MappingMode.fromString(modeStr), settings.get(KEY_DEFINITION_ID));
    }

    /**
     * Check whether settings contain a public source name declaration
     * (the user intent to create a derived index), regardless of whether
     * private binding fields have been enriched yet.
     */
    public static boolean hasDerivedSourceDeclaration(Settings settings) {
        String name = settings.get(KEY_SOURCE_NAME);
        return name != null && name.isEmpty() == false;
    }

    // ── Serialization ────────────────────────────────────────────────────

    /**
     * Emit only the PRIVATE (server-generated) binding settings for injection
     * into target IndexMetadata during creation. Public settings (source name,
     * definition ID) are already present from the user's create request.
     */
    public Settings toPrivateSettings() {
        return Settings.builder()
            .put(KEY_SOURCE_UUID, sourceUuid)
            .put(KEY_SOURCE_SHARDS, sourceShardCount)
            .put(KEY_SOURCE_ROUTING_SHARDS, sourceRoutingShardCount)
            .put(KEY_MAPPING_MODE, mappingMode.name())
            .build();
    }

    /**
     * Emit ALL binding settings (public + private). Used for internal
     * round-trip serialization and testing.
     */
    public Settings toSettings() {
        Settings.Builder builder = Settings.builder()
            .put(KEY_SOURCE_NAME, sourceName)
            .put(KEY_SOURCE_UUID, sourceUuid)
            .put(KEY_SOURCE_SHARDS, sourceShardCount)
            .put(KEY_SOURCE_ROUTING_SHARDS, sourceRoutingShardCount)
            .put(KEY_MAPPING_MODE, mappingMode.name());
        if (definitionId != null) {
            builder.put(KEY_DEFINITION_ID, definitionId);
        }
        return builder.build();
    }

    // ── Accessors ────────────────────────────────────────────────────────

    public String sourceName() {
        return sourceName;
    }

    public String sourceUuid() {
        return sourceUuid;
    }

    public Index sourceIndex() {
        return new Index(sourceName, sourceUuid);
    }

    public int sourceShardCount() {
        return sourceShardCount;
    }

    public int sourceRoutingShardCount() {
        return sourceRoutingShardCount;
    }

    public MappingMode mappingMode() {
        return mappingMode;
    }

    @Nullable
    public String definitionId() {
        return definitionId;
    }

    // ── Validation ───────────────────────────────────────────────────────

    public int resolveSourceShard(int targetShardId) {
        if (mappingMode != MappingMode.IDENTITY) {
            throw new UnsupportedOperationException("unsupported mapping mode: " + mappingMode);
        }
        if (targetShardId < 0 || targetShardId >= sourceShardCount) {
            throw new IllegalArgumentException(
                "target shard [" + targetShardId + "] out of range for source with " + sourceShardCount + " shards"
            );
        }
        return targetShardId;
    }

    public void validateTargetTopology(int targetShardCount) {
        if (mappingMode == MappingMode.IDENTITY && targetShardCount != sourceShardCount) {
            throw new IllegalStateException(
                "derived index binding requires target shards ["
                    + targetShardCount
                    + "] == source shards ["
                    + sourceShardCount
                    + "] for IDENTITY mapping"
            );
        }
    }

    public ValidationResult validateLive(IndexMetadata liveSourceMetadata) {
        if (liveSourceMetadata == null) {
            return ValidationResult.failure("source index [" + sourceName + "] does not exist in cluster state; rebuild required");
        }
        if (sourceUuid.equals(liveSourceMetadata.getIndexUUID()) == false) {
            return ValidationResult.failure(
                "source index ["
                    + sourceName
                    + "] UUID mismatch: bound=["
                    + sourceUuid
                    + "] live=["
                    + liveSourceMetadata.getIndexUUID()
                    + "]; source was recreated — rebuild required"
            );
        }
        if (sourceShardCount != liveSourceMetadata.getNumberOfShards()) {
            return ValidationResult.failure(
                "source index ["
                    + sourceName
                    + "] shard count changed: bound=["
                    + sourceShardCount
                    + "] live=["
                    + liveSourceMetadata.getNumberOfShards()
                    + "]; topology change — rebuild required"
            );
        }
        return ValidationResult.OK;
    }

    @org.opensearch.common.annotation.ExperimentalApi
    public static final class ValidationResult {
        public static final ValidationResult OK = new ValidationResult(true, null);

        private final boolean valid;
        private final String reason;

        private ValidationResult(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }

        public static ValidationResult failure(String reason) {
            return new ValidationResult(false, Objects.requireNonNull(reason));
        }

        public boolean isValid() {
            return valid;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return valid ? "OK" : "FAILED: " + reason;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DerivedIndexBinding other = (DerivedIndexBinding) o;
        return sourceShardCount == other.sourceShardCount
            && sourceRoutingShardCount == other.sourceRoutingShardCount
            && sourceName.equals(other.sourceName)
            && sourceUuid.equals(other.sourceUuid)
            && mappingMode == other.mappingMode
            && Objects.equals(definitionId, other.definitionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceName, sourceUuid, sourceShardCount, sourceRoutingShardCount, mappingMode, definitionId);
    }

    @Override
    public String toString() {
        return String.format(
            Locale.ROOT,
            "DerivedIndexBinding[source=%s/%s shards=%d/%d mode=%s definition=%s]",
            sourceName,
            sourceUuid,
            sourceShardCount,
            sourceRoutingShardCount,
            mappingMode,
            definitionId
        );
    }
}
