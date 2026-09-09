/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.mv.MVDefinitionValidator.ValidationResult;
import org.opensearch.mv.pull.MVPullSettings;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Map;

/**
 * Coordinator-side transport action backing {@code PUT /_mv/views/{name}}.
 * Validates the definition against the source index's real mapping (exactly as
 * {@code POST /_mv/_validate}), assembles the derived-target contract via the
 * shared {@link MVViewCreation} helper (so it is byte-identical to the
 * {@link MVViewsService.TargetCreator} auto-creation path), runs the fail-closed
 * {@link MVDefinitionResolver#validateCreation} gate, and submits a create-index
 * request. The derived-pull lifecycle starts the poller once the target shard is
 * STARTED — no extra orchestration here.
 */
public class TransportMVCreateViewAction extends HandledTransportAction<MVCreateViewRequest, MVCreateViewResponse> {

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportMVCreateViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client
    ) {
        super(MVCreateViewAction.NAME, transportService, actionFilters, MVCreateViewRequest::new, org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, MVCreateViewRequest request, ActionListener<MVCreateViewResponse> listener) {
        try {
            IndexMetadata sourceMetadata = clusterService.state().metadata().index(request.sourceIndex());
            if (sourceMetadata == null) {
                listener.onFailure(new ResourceNotFoundException("source index [" + request.sourceIndex() + "] does not exist"));
                return;
            }
            if (request.hasQueryText()) {
                listener.onFailure(
                    new IllegalArgumentException(
                        "PPL/SQL query-text definitions are not supported by the mv-data-format control plane in this "
                            + "version (they require the analytics-engine planner). Submit a compiled [descriptor] instead."
                    )
                );
                return;
            }

            final MVCompiledDefinition def;
            try {
                def = TransportMVValidateAction.parseAndCompile(request.descriptorJson());
            } catch (MVValidationReasons.ReasonedException e) {
                listener.onFailure(new IllegalArgumentException("[" + e.reasonCode() + "] " + e.getMessage()));
                return;
            }

            // Validate against the real source mapping BEFORE creating anything.
            Map<String, String> osTypes = TransportMVValidateAction.sourceOsTypes(sourceMetadata);
            ValidationResult vr = MVDefinitionValidator.validate(def, osTypes);
            if (vr.ok() == false) {
                String detail = vr.mismatches().isEmpty() ? "definition failed native validation" : String.join("; ", vr.mismatches());
                listener.onFailure(new IllegalArgumentException("[" + MVValidationReasons.SCHEMA_MISMATCH + "] " + detail));
                return;
            }

            String canonicalDescriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
            Settings settings = buildSettings(request, sourceMetadata.getNumberOfShards(), def, canonicalDescriptorJson);

            // Fail closed BEFORE submitting (same gate as the auto-creation path).
            try {
                MVDefinitionResolver.validateCreation(settings);
            } catch (RuntimeException e) {
                listener.onFailure(new IllegalArgumentException("[" + MVValidationReasons.CREATION_VALIDATION_FAILED + "] " + e.getMessage()));
                return;
            }

            String target = request.resolvedTargetIndex();
            CreateIndexRequest createRequest = new CreateIndexRequest(target).settings(settings)
                .mapping(MVViewCreation.targetMapping(def));

            client.admin()
                .indices()
                .create(
                    createRequest,
                    ActionListener.wrap(
                        r -> listener.onResponse(
                            new MVCreateViewResponse(
                                r.isAcknowledged(),
                                request.name(),
                                request.sourceIndex(),
                                target,
                                canonicalDescriptorJson,
                                def.stateColumnNames()
                            )
                        ),
                        listener::onFailure
                    )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Assemble the create-index settings: the shared derived-target contract
     * plus an optional {@code poll_interval} override. Package-private + pure so
     * a unit test can assert the exact settings map matches the Stage&nbsp;4
     * contract.
     */
    static Settings buildSettings(MVCreateViewRequest request, int sourceShards, MVCompiledDefinition def, String canonicalDescriptorJson) {
        Settings base = MVViewCreation.buildTargetSettings(request.sourceIndex(), sourceShards, def, canonicalDescriptorJson);
        if (request.pollInterval() == null || request.pollInterval().isBlank()) {
            return base;
        }
        return Settings.builder().put(base).put(MVPullSettings.PULL_INTERVAL.getKey(), request.pollInterval()).build();
    }
}
