/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.mv.MVDefinitionValidator.ValidationResult;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Coordinator-side transport action backing {@code POST /_mv/_validate}. Reads
 * the source index mapping from cluster state, resolves the definition input to
 * a compiled definition, then runs the {@link MVDefinitionValidator} native
 * cross-check — all read-only, no index is created or mutated.
 *
 * <p>The pure (non-native, non-cluster-state) steps — descriptor parsing,
 * ordering extraction, and success-response assembly — are factored into static
 * package-private helpers so they are unit-testable without the native library.
 */
public class TransportMVValidateAction extends HandledTransportAction<MVValidateRequest, MVValidateResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportMVValidateAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(MVValidateAction.NAME, transportService, actionFilters, MVValidateRequest::new, org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MVValidateRequest request, ActionListener<MVValidateResponse> listener) {
        try {
            listener.onResponse(validate(request, clusterService.state().metadata().index(request.sourceIndex())));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Perform the full validation for a request against a (possibly-null) source
     * index metadata. Returns a rejected {@link MVValidateResponse} for any
     * user-facing problem; only genuinely unexpected failures propagate.
     */
    MVValidateResponse validate(MVValidateRequest request, IndexMetadata sourceMetadata) {
        if (sourceMetadata == null) {
            return MVValidateResponse.rejected(
                MVValidationReasons.SOURCE_INDEX_NOT_FOUND,
                "source index [" + request.sourceIndex() + "] does not exist",
                List.of()
            );
        }
        if (request.hasQueryText()) {
            return MVValidateResponse.rejected(
                MVValidationReasons.QUERY_TEXT_PLANNING_UNAVAILABLE,
                "PPL/SQL query-text definitions require the analytics-engine planner, which is not wired into "
                    + "the mv-data-format control plane in this version. Submit a compiled [descriptor] instead. "
                    + "(A companion analytics-engine endpoint that plans query text into a descriptor and delegates "
                    + "to this action is the remaining work.)",
                List.of()
            );
        }

        final MVCompiledDefinition def;
        try {
            def = parseAndCompile(request.descriptorJson());
        } catch (MVValidationReasons.ReasonedException e) {
            return MVValidateResponse.rejected(e.reasonCode(), e.getMessage(), List.of());
        }

        Map<String, String> osTypes = sourceOsTypes(sourceMetadata);
        ValidationResult vr = MVDefinitionValidator.validate(def, osTypes);
        if (vr.ok()) {
            return buildSuccess(def, vr);
        }
        String reason = vr.nativeStateFields().isEmpty()
            ? MVValidationReasons.NATIVE_VALIDATION_REJECTED
            : MVValidationReasons.SCHEMA_MISMATCH;
        String message = vr.mismatches().isEmpty() ? "definition failed native validation" : vr.mismatches().get(0);
        return MVValidateResponse.rejected(reason, message, vr.mismatches());
    }

    // ── Pure, unit-testable helpers ──────────────────────────────────────

    /** Parse a descriptor JSON string and compile it, mapping failures to reason codes. */
    static MVCompiledDefinition parseAndCompile(String descriptorJson) {
        final MVDefinitionDescriptor descriptor;
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                descriptorJson
            )
        ) {
            descriptor = MVDefinitionDescriptor.fromXContent(parser);
        } catch (IOException | IllegalArgumentException e) {
            throw new MVValidationReasons.ReasonedException(
                MVValidationReasons.DESCRIPTOR_PARSE_FAILED,
                "descriptor is not a valid MV definition descriptor: " + e.getMessage()
            );
        }
        try {
            return MVCompiledDefinition.fromDescriptor(descriptor);
        } catch (RuntimeException e) {
            throw new MVValidationReasons.ReasonedException(
                MVValidationReasons.DESCRIPTOR_COMPILE_FAILED,
                "descriptor failed to compile: " + e.getMessage()
            );
        }
    }

    /** Extract source field -> OpenSearch mapping type from index metadata. */
    static Map<String, String> sourceOsTypes(IndexMetadata sourceMetadata) {
        MappingMetadata mapping = sourceMetadata.mapping();
        if (mapping == null) {
            return Map.of();
        }
        return MVSourceMappingReader.osTypes(mapping.sourceAsMap());
    }

    /** Assemble a successful validation response from a compiled definition and native result. */
    static MVValidateResponse buildSuccess(MVCompiledDefinition def, ValidationResult vr) {
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        return MVValidateResponse.valid(
            descriptorJson,
            def.stateColumnNames(),
            vr.nativeStateFields(),
            def.targetMapping(),
            orderingOf(def),
            vr.nativeSchemaHash()
        );
    }

    /** Extract the row-ordering contract of a compiled definition as response keys. */
    static List<MVValidateResponse.OrderKey> orderingOf(MVCompiledDefinition def) {
        List<MVValidateResponse.OrderKey> out = new ArrayList<>();
        for (MVGroupByOrdering.Key k : def.groupByOrdering().keys()) {
            out.add(
                new MVValidateResponse.OrderKey(
                    k.column(),
                    k.stateFieldIndex(),
                    k.direction().sqlToken(),
                    k.nullPlacement().sqlToken()
                )
            );
        }
        return out;
    }
}
