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
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.dataformat.MVWriterConfigRegistry;
import org.opensearch.index.remote.GetMVCheckpointTransportHandler;
import org.opensearch.index.remote.MVCheckpointService;
import org.opensearch.index.remote.MVRefreshListenerFactory;
import org.opensearch.index.remote.MVStateRefreshListener;
import org.opensearch.index.remote.MVStateRemoteManager;
import org.opensearch.index.remote.MVTargetHydrator;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * MV definition-layer + wiring plugin. Handles:
 * <ul>
 *   <li>MV definition CRUD (create/get/validate views)</li>
 *   <li>GetMVCheckpoint transport handler registration (source side)</li>
 *   <li>MVStateRefreshListener factory (source side — called by IndexShard.newEngineConfig)</li>
 *   <li>MVTargetHydrator lifecycle (target side — poll loop for hydrating target shards)</li>
 * </ul>
 */
public class MVEnginePlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(MVEnginePlugin.class);

    private volatile ClusterService clusterService;
    private volatile ThreadPool threadPool;
    private volatile TransportService transportService;
    private volatile Supplier<RepositoriesService> repositoriesServiceSupplier;

    /** Active target hydrators, keyed by target shard ID. */
    private final ConcurrentHashMap<ShardId, MVTargetHydrator> activeHydrators = new ConcurrentHashMap<>();

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
    public List<Setting<?>> getSettings() {
        return List.of(MVTargetHydrator.HYDRATE_INTERVAL);
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
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;

        MVDefinitionClusterService definitionService = new MVDefinitionClusterService(clusterService, client);

        // Register the MV writer-spec compiler
        MVWriterConfigRegistry.register(MVWriterConfig::fromCustomDataToRegistrySpecs);

        // ── Register GetMVCheckpoint transport handler (defect-#19 fix pattern) ──
        // Extract TransportService from NodeClient. The handler is registered directly
        // on TransportService.registerRequestHandler, not via ActionHandler, because
        // the request/response types are in the server module.
        if (client instanceof org.opensearch.transport.client.node.NodeClient nodeClient) {
            try {
                // NodeClient stores TransportService; access it for handler registration.
                // The field is in AbstractClient -> settings, but TransportService is injected.
                // Use the injected transportService from Guice. In OpenSearch, NodeClient
                // exposes it indirectly. We use reflection as a POC escape hatch.
                java.lang.reflect.Field tsField = org.opensearch.transport.client.node.NodeClient.class
                    .getDeclaredField("transportService");
                tsField.setAccessible(true);
                TransportService ts = (TransportService) tsField.get(nodeClient);
                this.transportService = ts;
                GetMVCheckpointTransportHandler.register(ts);
            } catch (Exception e) {
                logger.warn("Failed to register GetMVCheckpoint transport handler: {}", e.getMessage());
            }
        }

        // ── Register the MV refresh listener factory ──
        // IndexShard.newEngineConfig() calls this factory for shards with mv_definitions.
        MVRefreshListenerFactory.register((shardId, shardDataPath, primaryTerm, mvDefinitions, processedCheckpointSupplier) -> {
            MVStateRemoteManager remoteManager = resolveRemoteManager(shardId);
            if (remoteManager == null) {
                logger.warn("MV refresh listener: no remote manager for shard={}", shardId);
                return null;
            }
            MVStateRefreshListener listener = new MVStateRefreshListener(
                shardId,
                shardDataPath,
                remoteManager,
                primaryTerm,
                1L, // defMetadataVersion from cluster state
                mvDefinitions,
                processedCheckpointSupplier
            );
            // Init from remote (rebuild checkpoint for gen resume)
            listener.initFromRemote();
            // Register with MVCheckpointService
            MVCheckpointService.registerListener(shardId, listener);
            logger.info("MV refresh listener created and registered for shard={}", shardId);
            return listener;
        });

        return List.of(definitionService);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        // Register lifecycle listener for target hydration
        indexModule.addIndexEventListener(new MVShardLifecycleListener());
    }

    @Override
    public void close() {
        MVWriterConfigRegistry.unregister();
        MVRefreshListenerFactory.unregister();
        // Stop all active hydrators
        activeHydrators.values().forEach(MVTargetHydrator::close);
        activeHydrators.clear();
    }

    // ── Lifecycle listener for source cleanup + target hydration ─────────

    private class MVShardLifecycleListener implements IndexEventListener {

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            // Target hydration: start hydrator for shards with mv_binding
            if (!indexShard.routingEntry().primary()) {
                return;
            }
            IndexMetadata metadata = indexShard.indexSettings().getIndexMetadata();
            Map<String, String> mvBinding = metadata.getCustomData("mv_binding");
            if (mvBinding == null || mvBinding.isEmpty()) {
                return;
            }
            String sourceIndex = mvBinding.get("source_index");
            String mvId = mvBinding.get("mv_id");
            if (sourceIndex == null || mvId == null) {
                logger.warn("MV hydrator: mv_binding missing source_index or mv_id for shard={}", indexShard.shardId());
                return;
            }

            if (transportService == null) {
                logger.warn("MV hydrator: transport service not available for shard={}", indexShard.shardId());
                return;
            }

            MVStateRemoteManager remoteManager = resolveRemoteManager(indexShard.shardId());
            if (remoteManager == null) {
                logger.warn("MV hydrator: no remote manager for target shard={}", indexShard.shardId());
                return;
            }

            MVTargetHydrator hydrator = new MVTargetHydrator(
                indexShard.shardId(),
                mvId,
                sourceIndex,
                indexShard.shardPath().getDataPath(),
                transportService,
                clusterService,
                threadPool,
                remoteManager,
                MVTargetHydrator.HYDRATE_INTERVAL.get(indexShard.indexSettings().getSettings())
            );
            hydrator.start();
            activeHydrators.put(indexShard.shardId(), hydrator);
            logger.info("MV hydrator started for target shard={} source={} mvId={}", indexShard.shardId(), sourceIndex, mvId);
        }

        @Override
        public void afterIndexShardClosed(
            ShardId shardId,
            @Nullable IndexShard indexShard,
            Settings indexSettings
        ) {
            // Unregister source listener
            MVCheckpointService.unregisterListener(shardId);

            // Stop target hydrator
            MVTargetHydrator hydrator = activeHydrators.remove(shardId);
            if (hydrator != null) {
                hydrator.close();
                logger.info("MV hydrator stopped for closed shard={}", shardId);
            }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Resolve an MVStateRemoteManager for the given shard's index.
     * Uses the FS remote store repository if configured; returns null otherwise.
     */
    private MVStateRemoteManager resolveRemoteManager(ShardId shardId) {
        Supplier<RepositoriesService> repoSupplier = this.repositoriesServiceSupplier;
        if (repoSupplier == null) {
            return null;
        }
        try {
            RepositoriesService repoService = repoSupplier.get();
            if (clusterService == null) {
                return null;
            }
            IndexMetadata indexMetadata = clusterService.state().metadata().index(shardId.getIndex());
            if (indexMetadata == null) {
                return null;
            }
            // Get the remote store repo from index settings
            Settings indexSettings = indexMetadata.getSettings();
            String repoName = indexSettings.get("index.remote_store.segment.repository");
            if (repoName == null) {
                repoName = indexSettings.get("index.remote_store.repository");
            }
            if (repoName == null) {
                return null;
            }
            Repository repo = repoService.repository(repoName);
            if (!(repo instanceof BlobStoreRepository blobStoreRepo)) {
                return null;
            }
            BlobPath basePath = blobStoreRepo.basePath();
            return new MVStateRemoteManager(
                path -> blobStoreRepo.blobStore().blobContainer(path),
                new RemoteStorePathStrategy(PathType.FIXED),
                basePath,
                shardId.getIndex().getUUID()
            );
        } catch (Exception e) {
            logger.warn("Failed to resolve remote manager for shard={}: {}", shardId, e.getMessage());
            return null;
        }
    }
}
