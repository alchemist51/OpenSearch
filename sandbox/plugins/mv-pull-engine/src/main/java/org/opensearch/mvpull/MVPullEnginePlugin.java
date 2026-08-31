/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Pull-MV plugin: registers a target-owned shard build service for indices
 * declaring {@code index.mv_pull.source_index}. The target uses the generic
 * data-format-aware primary/replica engines; only a started primary runs the
 * remote poller and DataFusion artifact build.
 */
public class MVPullEnginePlugin extends Plugin {

    private volatile MVPullSettings.Services services;
    private volatile MVShardBuildService buildService;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            MVPullSettings.SOURCE_INDEX,
            MVPullSettings.PULL_INTERVAL,
            MVPullSettings.GROUP_FIELD,
            MVPullSettings.SUM_FIELD,
            MVPullSettings.DEFINITION
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
        this.services = new MVPullSettings.Services(clusterService, threadPool, repositoriesServiceSupplier);
        this.buildService = new MVShardBuildService(services);
        return List.of(buildService);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        MVShardBuildService service = buildService;
        if (service == null) {
            throw new IllegalStateException("mv_pull build service is not initialized");
        }
        indexModule.addIndexEventListener(service);
    }

    MVShardBuildService buildService() {
        return buildService;
    }
}
