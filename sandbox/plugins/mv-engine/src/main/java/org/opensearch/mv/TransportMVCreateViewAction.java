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
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for {@code PUT /_mv/views/{name}}: validates the descriptor,
 * creates the target index, and atomically writes the definition to both source
 * and target customData via {@link MVDefinitionClusterService}.
 */
public class TransportMVCreateViewAction extends HandledTransportAction<MVCreateViewRequest, MVCreateViewResponse> {

    private final MVDefinitionClusterService definitionService;

    @Inject
    public TransportMVCreateViewAction(
        TransportService transportService,
        ActionFilters actionFilters,
        MVDefinitionClusterService definitionService
    ) {
        super(MVCreateViewAction.NAME, transportService, actionFilters, MVCreateViewRequest::new,
            org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT);
        this.definitionService = definitionService;
    }

    @Override
    protected void doExecute(Task task, MVCreateViewRequest request, ActionListener<MVCreateViewResponse> listener) {
        try {
            // Parse descriptor
            MVDefinitionDescriptor descriptor = parseDescriptor(request.descriptorJson());
            MVCompiledDefinition compiledDef = MVCompiledDefinition.fromDescriptor(descriptor);

            definitionService.putDefinition(
                request.name(),
                request.sourceIndex(),
                descriptor,
                compiledDef,
                ActionListener.wrap(
                    result -> listener.onResponse(new MVCreateViewResponse(
                        true,
                        request.name(),
                        result.sourceIndex(),
                        result.targetIndex(),
                        result.descriptorJson(),
                        compiledDef.stateColumnNames()
                    )),
                    listener::onFailure
                )
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
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
