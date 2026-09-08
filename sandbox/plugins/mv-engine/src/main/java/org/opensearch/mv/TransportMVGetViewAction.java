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
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Transport action for {@code GET /_mv/views/{name}}: reads the target
 * index's customData binding and describes it.
 */
public class TransportMVGetViewAction extends HandledTransportAction<MVGetViewRequest, MVGetViewResponse> {

    private final ClusterService clusterService;

    @Inject
    public TransportMVGetViewAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(MVGetViewAction.NAME, transportService, actionFilters, MVGetViewRequest::new);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, MVGetViewRequest request, ActionListener<MVGetViewResponse> listener) {
        try {
            IndexMetadata target = clusterService.state().metadata().index(request.name());
            listener.onResponse(describe(request.name(), target));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    static MVGetViewResponse describe(String name, IndexMetadata target) {
        if (target == null) {
            return MVGetViewResponse.notFound(name);
        }
        Map<String, String> binding = target.getCustomData(MVConstants.TARGET_BINDING_KEY);
        if (binding == null) {
            return MVGetViewResponse.notFound(name);
        }
        String sourceIndex = binding.get("source");
        String descriptorJson = binding.get("descriptor");

        // Try to compile descriptor for metadata
        List<String> groupKeyNames = new ArrayList<>();
        List<String> aggregateNames = new ArrayList<>();
        List<String> stateFields = new ArrayList<>();
        boolean descriptorPresent = descriptorJson != null && descriptorJson.isBlank() == false;

        if (descriptorPresent) {
            try {
                var parser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent.createParser(
                    org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                    descriptorJson
                );
                MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.fromXContent(parser);
                MVCompiledDefinition compiled = MVCompiledDefinition.fromDescriptor(descriptor);
                for (GroupKey gk : compiled.groupKeys()) {
                    groupKeyNames.add(gk.name());
                }
                for (AggregateSpec agg : compiled.aggregates()) {
                    aggregateNames.add(agg.function().name() + "(" + (agg.sourceField() != null ? agg.sourceField() : "*") + ")");
                }
                stateFields = compiled.stateColumnNames();
            } catch (Exception e) {
                // Descriptor unreadable — still report what we can
            }
        }

        Settings settings = target.getSettings();
        String dataFormat = settings.get(MVConstants.DERIVED_DATA_FORMAT_SETTING, "");

        return new MVGetViewResponse(
            true, name, sourceIndex, dataFormat, "descriptor",
            descriptorPresent, groupKeyNames, aggregateNames, stateFields
        );
    }
}
