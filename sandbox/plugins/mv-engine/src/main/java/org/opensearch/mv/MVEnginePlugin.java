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
import java.util.Collections;
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
    private volatile Supplier<RepositoriesService> repositoriesServiceSupplier;

    /** Active target hydrators, keyed by target shard ID. */
    private final ConcurrentHashMap<ShardId, MVTargetHydrator> activeHydrators = new ConcurrentHashMap<>();

    private volatile MVReadService mvReadService;

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
            new RestMVValidateAction(),
            new RestMVQueryAction(mvReadService)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            MVTargetHydrator.HYDRATE_INTERVAL,
            // Target index settings stamped by MVViewCreation (must be registered for index creation)
            Setting.simpleString("index.mv.descriptor", Setting.Property.IndexScope, Setting.Property.Final),
            Setting.listSetting("index.mv.state_fields", Collections.emptyList(), s -> s, Setting.Property.IndexScope, Setting.Property.Final),
            Setting.simpleString("index.mv.colocate_with", Setting.Property.IndexScope, Setting.Property.Final),
            Setting.boolSetting("index.derived.enabled", false, Setting.Property.IndexScope, Setting.Property.Final),
            Setting.simpleString("index.derived.data_format", Setting.Property.IndexScope, Setting.Property.Final),
            Setting.simpleString("index.derived.source.name", Setting.Property.IndexScope, Setting.Property.Final),
            Setting.boolSetting("index.mv.state_merge_enabled", false, Setting.Property.IndexScope, Setting.Property.Final)
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
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;

        MVDefinitionClusterService definitionService = new MVDefinitionClusterService(clusterService, client);

        // Initialize the MV read service + native bridge
        this.mvReadService = new MVReadService(clusterService);
        MVNativeBridge.init();

        // Register the MV writer-spec compiler
        MVWriterConfigRegistry.register(MVWriterConfig::fromCustomDataToRegistrySpecs);

        // ── Transport handler registration moved to TransportMVCreateViewAction ──
        // The handler is registered from the @Inject constructor where TransportService
        // is available via Guice, eliminating the reflection hack.

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

    /**
     * Register the MV source refresh listener on a running shard.
     * Called either at shard start (if definitions already present) or
     * dynamically when definitions are added via cluster state update.
     */
    private void tryRegisterSourceRefreshListener(IndexShard indexShard, Map<String, String> mvDefs) {
        if (MVCheckpointService.getListener(indexShard.shardId()) != null) {
            logger.debug("MV source refresh listener already registered for shard={}", indexShard.shardId());
            return; // idempotent
        }
        org.apache.lucene.search.ReferenceManager.RefreshListener mvListener =
            MVRefreshListenerFactory.create(
                indexShard.shardId(),
                indexShard.shardPath().getDataPath(),
                indexShard.indexSettings().getIndexMetadata().primaryTerm(indexShard.shardId().id()),
                mvDefs,
                () -> {
                    try {
                        return indexShard.getProcessedLocalCheckpoint();
                    } catch (Exception e) {
                        return -1L;
                    }
                }
            );
        if (mvListener != null) {
            indexShard.addInternalRefreshListener(mvListener);
            logger.info("MV source refresh listener dynamically registered for shard={}", indexShard.shardId());
        }
    }

    private void tryStartTargetHydrator(IndexShard indexShard, Map<String, String> mvBinding) {
        if (activeHydrators.containsKey(indexShard.shardId())) {
            return;
        }
        String sourceIndex = mvBinding.get("source");
        String mvId = mvBinding.get("mv_id");
        if (sourceIndex == null || mvId == null) {
            logger.warn("MV hydrator: mv_binding missing source or mv_id for shard={}", indexShard.shardId());
            return;
        }

        TransportService ts = TransportMVCreateViewAction.getInjectedTransportService();
        if (ts == null) {
            logger.warn("MV hydrator: transport service not available for shard={}", indexShard.shardId());
            return;
        }

        // State files live under the SOURCE index UUID/repository, not the target.
        MVStateRemoteManager remoteManager = resolveRemoteManager(sourceIndex);
        if (remoteManager == null) {
            logger.warn("MV hydrator: no source remote manager for target={} source={}", indexShard.shardId(), sourceIndex);
            return;
        }

        MVTargetHydrator hydrator = new MVTargetHydrator(
            indexShard.shardId(),
            mvId,
            sourceIndex,
            indexShard.shardPath().getDataPath(),
            ts,
            clusterService,
            threadPool,
            remoteManager,
            MVTargetHydrator.HYDRATE_INTERVAL.get(indexShard.indexSettings().getSettings())
        );
        MVTargetHydrator existing = activeHydrators.putIfAbsent(indexShard.shardId(), hydrator);
        if (existing == null) {
            hydrator.start();
            logger.info("MV hydrator started for target shard={} source={} mvId={}", indexShard.shardId(), sourceIndex, mvId);
        } else {
            hydrator.close();
        }
    }

    private class MVShardLifecycleListener implements IndexEventListener {

        @Override
        public void afterIndexShardStarted(IndexShard indexShard) {
            if (!indexShard.routingEntry().primary()) {
                return;
            }

            // ── Source-side: dynamic MV refresh listener registration ────
            // If the source shard already has mv_definitions (e.g., view was created
            // before this session), register the listener now.
            // If not, add a ClusterStateListener to watch for definitions being added
            // so we can register the listener when they appear (create-source-then-view flow).
            IndexMetadata metadata = indexShard.indexSettings().getIndexMetadata();
            Map<String, String> mvDefs = metadata.getCustomData("mv_definitions");
            if (mvDefs != null && !mvDefs.isEmpty()) {
                tryRegisterSourceRefreshListener(indexShard, mvDefs);
            } else {
                // Watch for mv_definitions to appear via cluster state updates
                clusterService.addListener(new org.opensearch.cluster.ClusterStateListener() {
                    @Override
                    public void clusterChanged(org.opensearch.cluster.ClusterChangedEvent event) {
                        if (indexShard.state() == org.opensearch.index.shard.IndexShardState.CLOSED) {
                            clusterService.removeListener(this);
                            return;
                        }
                        IndexMetadata updatedMeta = event.state().metadata().index(indexShard.shardId().getIndex());
                        if (updatedMeta == null) {
                            clusterService.removeListener(this);
                            return;
                        }
                        Map<String, String> defs = updatedMeta.getCustomData("mv_definitions");
                        if (defs != null && !defs.isEmpty()) {
                            clusterService.removeListener(this);
                            tryRegisterSourceRefreshListener(indexShard, defs);
                        }
                    }
                });
            }

            // ── Target-side: dynamic binding/hydrator registration ──
            // Target creation happens before the atomic binding write, so the shard-start
            // callback commonly sees no mv_binding. Watch cluster state just as the source
            // side watches for mv_definitions.
            Map<String, String> mvBinding = metadata.getCustomData("mv_binding");
            if (mvBinding != null && !mvBinding.isEmpty()) {
                tryStartTargetHydrator(indexShard, mvBinding);
            } else {
                clusterService.addListener(new org.opensearch.cluster.ClusterStateListener() {
                    @Override
                    public void clusterChanged(org.opensearch.cluster.ClusterChangedEvent event) {
                        if (indexShard.state() == org.opensearch.index.shard.IndexShardState.CLOSED) {
                            clusterService.removeListener(this);
                            return;
                        }
                        IndexMetadata updatedMeta = event.state().metadata().index(indexShard.shardId().getIndex());
                        if (updatedMeta == null) {
                            clusterService.removeListener(this);
                            return;
                        }
                        Map<String, String> binding = updatedMeta.getCustomData("mv_binding");
                        if (binding != null && !binding.isEmpty()) {
                            clusterService.removeListener(this);
                            tryStartTargetHydrator(indexShard, binding);
                        }
                    }
                });
            }
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
        return resolveRemoteManager(shardId.getIndexName());
    }

    private MVStateRemoteManager resolveRemoteManager(String indexName) {
        Supplier<RepositoriesService> repoSupplier = this.repositoriesServiceSupplier;
        if (repoSupplier == null) {
            return null;
        }
        try {
            RepositoriesService repoService = repoSupplier.get();
            if (clusterService == null) {
                return null;
            }
            IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
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
                indexMetadata.getIndexUUID()
            );
        } catch (Exception e) {
            logger.warn("Failed to resolve remote manager for index={}: {}", indexName, e.getMessage());
            return null;
        }
    }
}
