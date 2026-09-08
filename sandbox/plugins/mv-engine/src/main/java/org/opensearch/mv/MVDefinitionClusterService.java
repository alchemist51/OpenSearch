/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Manages MV definitions in cluster state via atomic dual-write to
 * {@link IndexMetadata#getCustomData(String)}.
 *
 * <h2>Storage layout</h2>
 * <ul>
 *   <li><b>Source index</b> — {@code customData["mv_definitions"]} maps
 *       {@code mvId → descriptor JSON}.</li>
 *   <li><b>Target index</b> — {@code customData["mv_binding"]} maps
 *       {@code "source" → sourceIndex, "mv_id" → mvId,
 *       "descriptor" → descriptor JSON}.</li>
 * </ul>
 *
 * Both writes happen in ONE {@link ClusterStateUpdateTask}, so the cluster
 * state either has both or neither — no partial state.
 */
public class MVDefinitionClusterService {

    private static final Logger logger = LogManager.getLogger(MVDefinitionClusterService.class);

    private final ClusterService clusterService;
    private final Client client;

    public MVDefinitionClusterService(ClusterService clusterService, Client client) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
    }

    /**
     * Put an MV definition: validate, create target index, then atomically
     * write the definition into BOTH source and target customData.
     *
     * @param mvId           unique MV identifier (view name)
     * @param sourceIndex    the source index name
     * @param descriptor     the compiled definition's descriptor
     * @param compiledDef    the compiled definition
     * @param listener       callback
     */
    public void putDefinition(
        String mvId,
        String sourceIndex,
        MVDefinitionDescriptor descriptor,
        MVCompiledDefinition compiledDef,
        ActionListener<PutResult> listener
    ) {
        Objects.requireNonNull(mvId, "mvId");
        Objects.requireNonNull(sourceIndex, "sourceIndex");
        Objects.requireNonNull(descriptor, "descriptor");
        Objects.requireNonNull(compiledDef, "compiledDef");

        // Validate source exists
        IndexMetadata sourceMeta = clusterService.state().metadata().index(sourceIndex);
        if (sourceMeta == null) {
            listener.onFailure(new IllegalArgumentException("source index [" + sourceIndex + "] does not exist"));
            return;
        }

        // Validate definition against source mapping
        MappingMetadata mappingMeta = sourceMeta.mapping();
        if (mappingMeta != null) {
            Map<String, String> sourceOsTypes = MVSourceMappingReader.osTypes(mappingMeta.sourceAsMap());
            // Basic field existence check for group keys and aggregate source fields
            for (GroupKey key : compiledDef.groupKeys()) {
                if (key.isPlainColumn() && sourceOsTypes.containsKey(key.osFieldPath()) == false) {
                    listener.onFailure(new IllegalArgumentException(
                        String.format(Locale.ROOT, "group key field [%s] not found in source index [%s] mapping",
                            key.osFieldPath(), sourceIndex)));
                    return;
                }
            }
            for (AggregateSpec agg : compiledDef.aggregates()) {
                if (agg.sourceField() != null && sourceOsTypes.containsKey(agg.sourceField()) == false) {
                    listener.onFailure(new IllegalArgumentException(
                        String.format(Locale.ROOT, "aggregate source field [%s] not found in source index [%s] mapping",
                            agg.sourceField(), sourceIndex)));
                    return;
                }
            }
        }

        // Serialize descriptor
        String descriptorJson = serializeDescriptor(descriptor);
        String targetIndex = MVViewCreation.defaultTargetIndex(sourceIndex, mvId);

        // Step 1: Create target index
        Settings targetSettings = MVViewCreation.buildTargetSettings(
            sourceIndex, sourceMeta.getNumberOfShards(), compiledDef, descriptorJson
        );
        String targetMapping = MVViewCreation.targetMapping(compiledDef);

        CreateIndexRequest createRequest = new CreateIndexRequest(targetIndex)
            .settings(targetSettings)
            .mapping(targetMapping);

        client.admin().indices().create(createRequest, ActionListener.wrap(
            createResponse -> {
                // Step 2: Atomic dual-write to customData
                submitDualWrite(mvId, sourceIndex, targetIndex, descriptorJson, listener);
            },
            e -> {
                // If index already exists, still try the dual-write
                if (e instanceof org.opensearch.ResourceAlreadyExistsException) {
                    logger.info("target index [{}] already exists, proceeding with dual-write", targetIndex);
                    submitDualWrite(mvId, sourceIndex, targetIndex, descriptorJson, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        ));
    }

    /**
     * Remove an MV definition: atomically remove from both source and target customData.
     */
    public void removeDefinition(String mvId, String sourceIndex, ActionListener<Boolean> listener) {
        String targetIndex = MVViewCreation.defaultTargetIndex(sourceIndex, mvId);
        clusterService.submitStateUpdateTask(
            "remove-mv-definition [" + mvId + "]",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());

                    // Remove from source
                    IndexMetadata sourceMeta = metadata.getSafe(currentState.metadata().index(sourceIndex).getIndex());
                    Map<String, String> defs = new HashMap<>(
                        sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY) != null
                            ? sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY)
                            : Map.of()
                    );
                    defs.remove(mvId);
                    metadata.put(IndexMetadata.builder(sourceMeta).putCustom(MVConstants.SOURCE_DEFINITIONS_KEY, defs));

                    // Remove from target (if it exists)
                    IndexMetadata targetMeta = currentState.metadata().index(targetIndex);
                    if (targetMeta != null) {
                        IndexMetadata.Builder targetBuilder = IndexMetadata.builder(targetMeta);
                        targetBuilder.removeCustom(MVConstants.TARGET_BINDING_KEY);
                        metadata.put(targetBuilder);
                    }

                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(true);
                }
            }
        );
    }

    /**
     * Read the MV definition for a given mvId from the source index's customData.
     *
     * @return the descriptor JSON, or null if not found
     */
    public String getDefinition(String mvId, String sourceIndex) {
        IndexMetadata sourceMeta = clusterService.state().metadata().index(sourceIndex);
        if (sourceMeta == null) {
            return null;
        }
        Map<String, String> defs = sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY);
        if (defs == null) {
            return null;
        }
        return defs.get(mvId);
    }

    /**
     * List all MV definitions on a source index.
     *
     * @return map of mvId → descriptor JSON
     */
    public Map<String, String> listDefinitions(String sourceIndex) {
        IndexMetadata sourceMeta = clusterService.state().metadata().index(sourceIndex);
        if (sourceMeta == null) {
            return Map.of();
        }
        Map<String, String> defs = sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY);
        return defs != null ? Map.copyOf(defs) : Map.of();
    }

    // ── Internal ─────────────────────────────────────────────────────────

    private void submitDualWrite(
        String mvId,
        String sourceIndex,
        String targetIndex,
        String descriptorJson,
        ActionListener<PutResult> listener
    ) {
        clusterService.submitStateUpdateTask(
            "put-mv-definition [" + mvId + "]",
            new ClusterStateUpdateTask(Priority.NORMAL) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    Metadata.Builder metadata = Metadata.builder(currentState.metadata());

                    // Source: merge mvId → descriptorJson into mv_definitions
                    IndexMetadata sourceMeta = currentState.metadata().index(sourceIndex);
                    if (sourceMeta == null) {
                        throw new IllegalStateException("source index [" + sourceIndex + "] disappeared during update");
                    }
                    Map<String, String> defs = new HashMap<>(
                        sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY) != null
                            ? sourceMeta.getCustomData(MVConstants.SOURCE_DEFINITIONS_KEY)
                            : Map.of()
                    );
                    defs.put(mvId, descriptorJson);
                    metadata.put(IndexMetadata.builder(sourceMeta).putCustom(MVConstants.SOURCE_DEFINITIONS_KEY, defs));

                    // Target: write mv_binding
                    IndexMetadata targetMeta = currentState.metadata().index(targetIndex);
                    if (targetMeta == null) {
                        throw new IllegalStateException("target index [" + targetIndex + "] disappeared during update");
                    }
                    Map<String, String> binding = new LinkedHashMap<>();
                    binding.put("source", sourceIndex);
                    binding.put("mv_id", mvId);
                    binding.put("descriptor", descriptorJson);
                    metadata.put(IndexMetadata.builder(targetMeta).putCustom(MVConstants.TARGET_BINDING_KEY, binding));

                    return ClusterState.builder(currentState).metadata(metadata).build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    // ── POC: no close/open needed ──
                    // The MV definition is now in IndexMetadata.customData("mv_definitions").
                    // The next writer rotation (on the next ingest + refresh cycle) will
                    // pick up the definition via MVWriterConfigRegistry.compileSpecs().
                    // Close/open is unnecessary for the create-view-then-ingest flow and
                    // causes translog recovery crashes when the Rust finalizeWriter is
                    // called on an MV-registered writer with 0 rows.
                    // Production will use a dynamic writer-level MV builder registration
                    // (IndexSettings update listener or writer-pool rotation) so that
                    // definitions added to an already-ingesting source are picked up
                    // within one refresh interval without destructive close/open.
                    logger.info("MV definition [{}] applied to source index [{}] metadata — next writer rotation will activate builders",
                        mvId, sourceIndex);
                    listener.onResponse(new PutResult(mvId, sourceIndex, targetIndex, descriptorJson));
                }
            }
        );
    }

    private static String serializeDescriptor(MVDefinitionDescriptor descriptor) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize MV definition descriptor", e);
        }
    }

    /** Result of a successful put operation. */
    public record PutResult(String mvId, String sourceIndex, String targetIndex, String descriptorJson) {}
}
