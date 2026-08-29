/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingAction;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;

/** Rejects user mapping mutation on schema-managed derived targets. */
final class DerivedIndexActionFilter implements ActionFilter {

    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    DerivedIndexActionFilter(ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver) {
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    @Override
    public int order() {
        return 1;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
        Task task,
        String action,
        Request request,
        ActionRequestMetadata<Request, Response> actionRequestMetadata,
        ActionListener<Response> listener,
        ActionFilterChain<Request, Response> chain
    ) {
        if (PutMappingAction.NAME.equals(action) && request instanceof PutMappingRequest putMappingRequest) {
            for (String index : indexNameExpressionResolver.concreteIndexNames(clusterService.state(), putMappingRequest)) {
                var metadata = clusterService.state().metadata().index(index);
                if (metadata != null && metadata.getSettings().getAsBoolean(MVConstants.DERIVED_INDEX_SETTING, false)) {
                    listener.onFailure(
                        new UnsupportedOperationException(
                            "derived index [" + index + "] has a replication-managed mapping; update the materialized-view definition"
                        )
                    );
                    return;
                }
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
