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
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport action for {@code POST /_mv/_validate}: dry-run compile and
 * validate a definition against the source index mapping. Read-only — never
 * creates or mutates any index. Native cross-check deferred to commit 2
 * (TODO: add MVNativeBridge validation).
 */
public class TransportMVValidateAction extends HandledTransportAction<MVValidateRequest, MVValidateResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportMVValidateAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(MVValidateAction.NAME, transportService, actionFilters, MVValidateRequest::new,
            org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT);
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

    static MVValidateResponse validate(MVValidateRequest request, IndexMetadata sourceMetadata) throws IOException {
        if (sourceMetadata == null) {
            return MVValidateResponse.rejected(
                MVValidationReasons.SOURCE_INDEX_NOT_FOUND,
                "source index [" + request.sourceIndex() + "] does not exist",
                List.of()
            );
        }

        // Parse descriptor
        MVDefinitionDescriptor descriptor;
        try {
            descriptor = parseDescriptor(request.descriptorJson());
        } catch (Exception e) {
            return MVValidateResponse.rejected(
                MVValidationReasons.DESCRIPTOR_PARSE_FAILED,
                "descriptor parse failed: " + e.getMessage(),
                List.of()
            );
        }

        // Compile
        MVCompiledDefinition compiledDef;
        try {
            compiledDef = MVCompiledDefinition.fromDescriptor(descriptor);
        } catch (Exception e) {
            return MVValidateResponse.rejected(
                MVValidationReasons.DESCRIPTOR_COMPILE_FAILED,
                "descriptor compile failed: " + e.getMessage(),
                List.of()
            );
        }

        // Validate fields against source mapping
        MappingMetadata mappingMeta = sourceMetadata.mapping();
        if (mappingMeta != null) {
            Map<String, String> sourceOsTypes = MVSourceMappingReader.osTypes(mappingMeta.sourceAsMap());
            List<String> mismatches = new ArrayList<>();
            for (GroupKey key : compiledDef.groupKeys()) {
                if (key.isPlainColumn() && sourceOsTypes.containsKey(key.osFieldPath()) == false) {
                    mismatches.add("group key [" + key.osFieldPath() + "] not found in source mapping");
                }
            }
            for (AggregateSpec agg : compiledDef.aggregates()) {
                if (agg.sourceField() != null && sourceOsTypes.containsKey(agg.sourceField()) == false) {
                    mismatches.add("aggregate field [" + agg.sourceField() + "] not found in source mapping");
                }
            }
            if (mismatches.isEmpty() == false) {
                return MVValidateResponse.rejected(
                    MVValidationReasons.CREATION_VALIDATION_FAILED,
                    "definition references fields not in source mapping",
                    mismatches
                );
            }
        }

        // Build success response
        // TODO: native cross-check via MVNativeBridge (commit 2)
        return buildSuccessResponse(descriptor, compiledDef);
    }

    static MVValidateResponse buildSuccessResponse(MVDefinitionDescriptor descriptor, MVCompiledDefinition compiledDef)
        throws IOException {
        // Serialize descriptor with integrity hash
        MVDefinitionDescriptor withHash = compiledDef.toDescriptor();
        String descriptorJson;
        try (var builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            withHash.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            descriptorJson = org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString();
        }

        List<String> stateFields = compiledDef.stateColumnNames();
        Map<String, String> targetMapping = compiledDef.targetMapping();
        List<MVValidateResponse.OrderKey> ordering = new ArrayList<>();
        for (MVGroupByOrdering.Key key : compiledDef.groupByOrdering().keys()) {
            ordering.add(new MVValidateResponse.OrderKey(
                key.column(), key.stateFieldIndex(),
                key.direction().sqlToken(), key.nullPlacement().sqlToken()
            ));
        }

        return MVValidateResponse.valid(descriptorJson, stateFields, List.of(), targetMapping, ordering, 0L);
    }

    private static MVDefinitionDescriptor parseDescriptor(String json) throws IOException {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("descriptor is required");
        }
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(
            NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS, json
        )) {
            return MVDefinitionDescriptor.fromXContent(parser);
        }
    }
}
