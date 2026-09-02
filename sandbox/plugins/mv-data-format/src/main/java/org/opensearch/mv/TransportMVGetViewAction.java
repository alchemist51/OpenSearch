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
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.List;

/**
 * Coordinator-side transport action backing {@code GET /_mv/views/{name}}. Reads
 * the target index's settings from cluster state and summarizes its binding +
 * persisted descriptor. Read-only.
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

    /**
     * Build the describe response from a (possibly-null) target index metadata.
     * Pure Java — {@link MVDefinitionResolver#descriptorFromSettings} does not
     * touch the native library — so it is unit-testable.
     */
    static MVGetViewResponse describe(String name, IndexMetadata target) {
        if (target == null) {
            return MVGetViewResponse.notFound(name);
        }
        Settings settings = target.getSettings();
        String dataFormat = DerivedIndexBinding.dataFormatCategory(settings);
        if (MVDataFormat.NAME.equals(dataFormat) == false) {
            // Not an MV derived target.
            return MVGetViewResponse.notFound(name);
        }
        String sourceIndex = settings.get(DerivedIndexBinding.KEY_SOURCE_NAME);
        String definitionLabel = MVDefinitionResolver.definitionLabel(settings);

        MVDefinitionDescriptor descriptor = MVDefinitionResolver.descriptorFromSettings(settings);
        boolean descriptorPresent = descriptor != null;
        List<String> groupKeys = new ArrayList<>();
        List<String> aggregates = new ArrayList<>();
        if (descriptor != null) {
            for (MVDefinitionDescriptor.GroupKeyDescriptor k : descriptor.groupKeys()) {
                groupKeys.add(k.name());
            }
            for (MVDefinitionDescriptor.AggregateDescriptor a : descriptor.aggregates()) {
                aggregates.add(a.alias());
            }
        }
        List<String> stateFields = settings.getAsList(MVConstants.STATE_FIELDS_SETTING);

        return new MVGetViewResponse(
            true,
            name,
            sourceIndex,
            dataFormat,
            definitionLabel,
            descriptorPresent,
            groupKeys,
            aggregates,
            stateFields
        );
    }
}
