/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.engine.dataformat.MVWriterConfigRegistry;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * MV definition-layer plugin: stores MV definitions as JSON in
 * IndexMetadata.customData on BOTH source and target indices, written
 * atomically in ONE ClusterStateUpdateTask.
 *
 * <p>Registers create/get/validate transport actions and REST handlers.
 * No native code, no pull/ship/merge — definition layer only.</p>
 */
public class MVEnginePlugin extends Plugin implements ActionPlugin {

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(MVCreateViewAction.INSTANCE, TransportMVCreateViewAction.class),
            new ActionHandler<>(MVGetViewAction.INSTANCE, TransportMVGetViewAction.class),
            new ActionHandler<>(MVValidateAction.INSTANCE, TransportMVValidateAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(
            new RestMVViewAction(),
            new RestMVValidateAction()
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        MVDefinitionClusterService definitionService = new MVDefinitionClusterService(clusterService, client);
        // Register the MV writer-spec compiler so parquet-data-format can compile
        // MV definitions from customData into FFI-ready specs without depending on mv-engine.
        MVWriterConfigRegistry.register(MVWriterConfig::fromCustomDataToRegistrySpecs);
        return List.of(definitionService);
    }

    @Override
    public void close() {
        MVWriterConfigRegistry.unregister();
    }
}
