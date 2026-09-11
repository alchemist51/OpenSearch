/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.derived.pull.NodeDerivedPullService;
import org.opensearch.mv.pull.MVBuildRuntime;
import org.opensearch.mv.pull.MVDerivedPullFormat;
import org.opensearch.mv.pull.MVPullSettings;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.Plugin;

/**
 * Unified materialized-view plugin — CONTROL PLANE + PULL PIPELINE ONLY.
 *
 * <p>This plugin registers NO data formats and NO indexing engine. An MV
 * target is a standard derived index ({@code index.derived.enabled=true})
 * on the stock composite store (primary {@code parquet}, secondary
 * {@code lucene}); its state artifacts are plain parquet generations
 * published through the generic
 * {@code IndexShard#publishDerivedArtifact(String, WriterFileSet, Map)}
 * seam and owned end-to-end (catalog, checksum, upload, recovery) by the
 * stock machinery.
 *
 * <p>What lives here is exclusively MV-domain logic: the definition
 * compiler/validator and its REST control plane, the pull checkpoint
 * protocol (target-driven request/reply against the source primary), the
 * DataFusion fold runtime, noop/coverage tracking, watermark accounting,
 * and shard colocation. The derived data-format CATEGORY value
 * ({@code index.derived.data_format=materialized_view}) survives purely as
 * control-plane routing — pull-service eligibility and analytics MV-serving
 * dispatch key off it; it resolves to no physical format.</p>
 */
public class MVDataFormatPlugin extends Plugin
    implements
        org.opensearch.plugins.ClusterPlugin,
        org.opensearch.plugins.ActionPlugin,
        org.opensearch.plugins.ExtensiblePlugin,
        org.opensearch.plugins.CircuitBreakerPlugin {

    private volatile org.opensearch.transport.client.Client client;
    private volatile org.opensearch.cluster.service.ClusterService clusterService;
    private volatile org.opensearch.action.support.ActionFilter derivedIndexActionFilter;
    private volatile NodeDerivedPullService pullService;
    private volatile NodeRoutingSnapshotService routingSnapshotService;
    private volatile MVReplicationService replicationService;
    /** Stage 2: native DataFusionRuntime pointer within MV's classloader. */
    private volatile long mvNativeRuntimePtr;
    /** Stage 5: circuit breaker for MV pull build memory accounting. */
    private volatile org.opensearch.core.common.breaker.CircuitBreaker mvPullBreaker;
    /** Defect 13: per-shard noop seqNo tracker for coverage correction. */
    private volatile MVNoopTracker noopTracker;

    /** Stage 5: node-scope limit for the MV pull circuit breaker. */
    public static final org.opensearch.common.settings.Setting<Long> MV_PULL_BREAKER_LIMIT = org.opensearch.common.settings.Setting
        .longSetting(
            "mv_pull.breaker.limit_bytes",
            128L * 1024 * 1024,
            0L,
            org.opensearch.common.settings.Setting.Property.NodeScope,
            org.opensearch.common.settings.Setting.Property.Dynamic
        );

    /**
     * Node-scope byte limit for the MV managed DataFusion memory pool.
     * {@code 0b} (default) means heap/4, giving the streaming build's
     * aggregation and external sort real headroom before spilling.
     */
    public static final org.opensearch.common.settings.Setting<org.opensearch.core.common.unit.ByteSizeValue> MV_NATIVE_POOL_LIMIT =
        org.opensearch.common.settings.Setting.byteSizeSetting(
            "mv_pull.native_pool_limit_bytes",
            org.opensearch.core.common.unit.ByteSizeValue.ZERO,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

    /**
     * Node-scope spill directory for MV managed builds. Empty (default) means
     * {@code <first data path>/mv-spill}, created at startup — spill is ALWAYS
     * enabled so large builds spill to disk instead of failing at the pool
     * ceiling (the DE7 saturation failure mode).
     */
    public static final org.opensearch.common.settings.Setting<String> MV_SPILL_DIRECTORY = org.opensearch.common.settings.Setting
        .simpleString("mv_pull.spill_directory", "", org.opensearch.common.settings.Setting.Property.NodeScope);

    /** Node-scope disk budget for MV spill. {@code 0b} = DataFusion default sizing. */
    public static final org.opensearch.common.settings.Setting<org.opensearch.core.common.unit.ByteSizeValue> MV_SPILL_DISK_LIMIT =
        org.opensearch.common.settings.Setting.byteSizeSetting(
            "mv_pull.spill_disk_limit_bytes",
            org.opensearch.core.common.unit.ByteSizeValue.ZERO,
            org.opensearch.common.settings.Setting.Property.NodeScope
        );

    public MVDataFormatPlugin() {}

    @SuppressWarnings("deprecation") // SOURCE_INDEX registered for BWC only
    @Override
    public java.util.Collection<Object> createComponents(
        org.opensearch.transport.client.Client client,
        org.opensearch.cluster.service.ClusterService clusterService,
        org.opensearch.threadpool.ThreadPool threadPool,
        org.opensearch.watcher.ResourceWatcherService resourceWatcherService,
        org.opensearch.script.ScriptService scriptService,
        org.opensearch.core.xcontent.NamedXContentRegistry xContentRegistry,
        org.opensearch.env.Environment environment,
        org.opensearch.env.NodeEnvironment nodeEnvironment,
        org.opensearch.core.common.io.stream.NamedWriteableRegistry namedWriteableRegistry,
        org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
        java.util.function.Supplier<org.opensearch.repositories.RepositoriesService> repositoriesServiceSupplier
    ) {
        this.client = client;
        this.clusterService = clusterService;
        this.derivedIndexActionFilter = new DerivedIndexActionFilter(clusterService, indexNameExpressionResolver);
        // Cluster-applier-safe routing snapshot: engine callbacks read this
        // instead of calling clusterService.state() which would deadlock.
        // Use nodeEnvironment.nodeId() instead of clusterService.localNode().getId()
        // because clusterService.state() is not initialized during createComponents().
        this.routingSnapshotService = new NodeRoutingSnapshotService(nodeEnvironment.nodeId());
        this.routingSnapshotService.bind(clusterService);
        // D20/D23/D24: auto-create MV target indices for sources declaring
        // index.mv.views (cluster-manager only; tolerant of re-entry).
        clusterService.addListener(new MVViewsService.TargetCreator(client));

        // ── Generic pull service with MV SPI adapter ─────────────────────
        // Wire the format-agnostic NodeDerivedPullService with the MV-specific
        // DerivedPullFormat implementation. The generic service owns one poller
        // per eligible local target primary shard — no MV-specific orchestration
        // code is needed.

        // Stage 2: create a shared DataFusionRuntime within this plugin's
        // native instance for managed MV builds. The MV classloader has its
        // own native globals (separate from the DF plugin), so we must
        // initialize the runtime manager and create a runtime here.
        // The runtime is shared across all MV builds on this node.
        long mvRuntimePtr = 0L;
        try {
            MVNativeBridge.initRuntime(Runtime.getRuntime().availableProcessors());
            // Node-scoped settings (committed defaults, no env overrides):
            // pool = mv_pull.native_pool_limit_bytes (default heap/4 so the
            // streaming build's aggregation + external sort have headroom);
            // spill dir = mv_pull.spill_directory (default <data path>/mv-spill)
            // so large builds SPILL to disk instead of failing at the pool
            // ceiling — the DE7 saturation failure mode.
            long mvPoolLimit = MV_NATIVE_POOL_LIMIT.get(environment.settings()).getBytes();
            if (mvPoolLimit <= 0L) {
                mvPoolLimit = Runtime.getRuntime().maxMemory() / 4;
            }
            String spillDir = MV_SPILL_DIRECTORY.get(environment.settings());
            if (spillDir == null || spillDir.isEmpty()) {
                java.nio.file.Path p = environment.dataFiles()[0].resolve("mv-spill");
                java.nio.file.Files.createDirectories(p);
                spillDir = p.toAbsolutePath().toString();
            } else {
                java.nio.file.Files.createDirectories(java.nio.file.Path.of(spillDir));
            }
            long spillLimit = MV_SPILL_DISK_LIMIT.get(environment.settings()).getBytes();
            mvRuntimePtr = MVNativeBridge.createGlobalRuntime(mvPoolLimit, spillDir, spillLimit);
            org.apache.logging.log4j.LogManager.getLogger(MVDataFormatPlugin.class)
                .info("mv_pull: managed runtime pool={} bytes spill_dir=[{}] spill_limit={} bytes", mvPoolLimit, spillDir, spillLimit);
        } catch (Exception e) {
            // Non-fatal: fall back to 0 (MVBuildRuntime creation will fail
            // gracefully at build time with a clear error).
            org.apache.logging.log4j.LogManager.getLogger(MVDataFormatPlugin.class)
                .warn("mv_pull: failed to create managed DataFusion runtime, builds will fail", e);
        }
        this.mvNativeRuntimePtr = mvRuntimePtr;

        // Stage 5: wire the actual MV pull circuit breaker (set by the
        // framework via setCircuitBreaker before createComponents runs).
        MVPullSettings.Services mvServices = new MVPullSettings.Services(
            clusterService,
            threadPool,
            repositoriesServiceSupplier,
            mvRuntimePtr,
            mvPullBreaker,
            client
        );
        MVDerivedPullFormat mvFormat = new MVDerivedPullFormat(mvServices);
        // Engine-driven compaction of pull targets: the target's merge scheduler
        // hands published generations to the definition-aware state merger.
        org.opensearch.index.engine.derived.pull.spi.DerivedStateMergers.register(
            mvFormat.formatId(),
            (indexSettings, shardId, shardDataPath) -> new org.opensearch.mv.pull.MVStateCompactionMerger(
                indexSettings,
                shardId,
                shardDataPath,
                mvServices
            )
        );
        this.pullService = new NodeDerivedPullService(threadPool, java.util.List.of(mvFormat));
        this.pullService.start();

        // ── Noop tracking service (request-driven model) ───────────────────
        // In the request-driven model, the TARGET drives checkpoint acquisition
        // by sending MVCheckpointRequestAction to the source primary every poll
        // round. This service only owns the noop tracker and cleans up on shard
        // close. The checkpoint request handler does the full scoped construction.
        this.noopTracker = new MVNoopTracker();
        this.replicationService = new MVReplicationService(noopTracker);

        return java.util.List.of(pullService, noopTracker);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        NodeDerivedPullService service = pullService;
        if (service == null) {
            throw new IllegalStateException("mv_pull: pull service is not initialized");
        }
        indexModule.addIndexEventListener(service);
        // Register the replication service as an event listener so it can
        // track source shard starts/closes without modifying the engine.
        MVReplicationService replService = replicationService;
        if (replService != null) {
            indexModule.addIndexEventListener(replService);
        }
        // Defect 13: register the noop indexing listener on ALL indices.
        // On source indices, the listener records seqNos for failed index ops
        // and all delete ops — these consume seqNos without producing parquet
        // rows and would otherwise cause permanent coverage gate failure.
        // On non-source indices the listener fires but noops are never
        // queried (the tracker is keyed by shardId and only source shards
        // are checked during checkpoint construction).
        MVNoopTracker tracker = noopTracker;
        if (tracker != null) {
            indexModule.addIndexOperationListener(new MVNoopIndexingListener(tracker));
        }
    }

    @SuppressWarnings("deprecation") // SOURCE_INDEX registered for BWC only
    @Override
    public java.util.List<org.opensearch.common.settings.Setting<?>> getSettings() {
        java.util.List<org.opensearch.common.settings.Setting<?>> base = java.util.List.of(
            org.opensearch.common.settings.Setting.simpleString(
                MVConstants.COLOCATE_WITH_SETTING,
                org.opensearch.common.settings.Setting.Property.IndexScope
            ),
            // Stage 4: persisted, self-contained MV definition descriptor JSON.
            // Public + Final + IndexScope so the MV control plane can submit it
            // in the create request (like index.derived.definition_id).
            MVDefinitionResolver.DESCRIPTOR_SETTING,
            org.opensearch.common.settings.Setting.boolSetting(
                MVConstants.DERIVED_INDEX_SETTING,
                false,
                org.opensearch.common.settings.Setting.Property.IndexScope,
                org.opensearch.common.settings.Setting.Property.Final
            ),
            org.opensearch.common.settings.Setting.listSetting(
                MVConstants.STATE_FIELDS_SETTING,
                java.util.List.of(),
                java.util.function.Function.identity(),
                org.opensearch.common.settings.Setting.Property.IndexScope
            ),
            org.opensearch.common.settings.Setting.listSetting(
                MVConstants.VIEWS_SETTING,
                java.util.List.of(),
                java.util.function.Function.identity(),
                org.opensearch.common.settings.Setting.Property.IndexScope
            ),
            // Pull-model settings (BWC registration only)
            MVPullSettings.SOURCE_INDEX,
            MVPullSettings.PULL_INTERVAL,
            MVPullSettings.DEFINITION_HASH,
            // Stage 2: managed build runtime settings
            MVBuildRuntime.MV_SPILL_BUDGET_BYTES,
            MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT,
            MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE,
            // Stage 5: circuit breaker limit
            MV_PULL_BREAKER_LIMIT,
            // Bench pre-flight: managed native runtime sizing + always-on spill
            MV_NATIVE_POOL_LIMIT,
            MV_SPILL_DIRECTORY,
            MV_SPILL_DISK_LIMIT
        );
        java.util.List<org.opensearch.common.settings.Setting<?>> all = new java.util.ArrayList<>(base);
        all.addAll(MVPullSettings.admissionSettings());
        return java.util.Collections.unmodifiableList(all);
    }

    @Override
    public java.util.Collection<org.opensearch.index.shard.IndexSettingProvider> getAdditionalIndexSettingProviders() {
        return java.util.List.of(new MVViewsService.Provider());
    }

    @Override
    public java.util.Collection<org.opensearch.cluster.routing.allocation.decider.AllocationDecider> createAllocationDeciders(
        org.opensearch.common.settings.Settings settings,
        org.opensearch.common.settings.ClusterSettings clusterSettings
    ) {
        return java.util.List.of(new MVColocationAllocationDecider());
    }

    @Override
    public
        java.util.List<ActionHandler<? extends org.opensearch.action.ActionRequest, ? extends org.opensearch.core.action.ActionResponse>>
        getActions() {
        return java.util.List.of(
            // Checkpoint request: target request-driven checkpoint fetch from source.
            new ActionHandler<>(MVCheckpointRequestAction.INSTANCE, MVCheckpointRequestTransportHandler.class),
            // Stage 5: MV definition control plane (validate + view CRUD).
            new ActionHandler<>(MVValidateAction.INSTANCE, TransportMVValidateAction.class),
            new ActionHandler<>(MVCreateViewAction.INSTANCE, TransportMVCreateViewAction.class),
            new ActionHandler<>(MVGetViewAction.INSTANCE, TransportMVGetViewAction.class)
        );
    }

    /**
     * Stage 5: REST endpoints for the MV definition control plane.
     * {@code POST /_mv/_validate} (dry-run compile + validate) and
     * {@code PUT/GET/DELETE /_mv/views/{name}} (view CRUD).
     */
    @Override
    public java.util.List<org.opensearch.rest.RestHandler> getRestHandlers(
        org.opensearch.common.settings.Settings settings,
        org.opensearch.rest.RestController restController,
        org.opensearch.common.settings.ClusterSettings clusterSettings,
        org.opensearch.common.settings.IndexScopedSettings indexScopedSettings,
        org.opensearch.common.settings.SettingsFilter settingsFilter,
        org.opensearch.cluster.metadata.IndexNameExpressionResolver indexNameExpressionResolver,
        java.util.function.Supplier<org.opensearch.cluster.node.DiscoveryNodes> nodesInCluster
    ) {
        return java.util.List.of(new RestMVValidateAction(), new RestMVViewAction());
    }

    @Override
    public java.util.List<org.opensearch.action.support.ActionFilter> getActionFilters() {
        return derivedIndexActionFilter == null ? java.util.List.of() : java.util.List.of(derivedIndexActionFilter);
    }

    /** Accessor for integration tests that verify poller lifecycle. */
    public NodeDerivedPullService pullService() {
        return pullService;
    }

    // ---- CircuitBreakerPlugin (Stage 5) ----

    @Override
    public org.opensearch.indices.breaker.BreakerSettings getCircuitBreaker(org.opensearch.common.settings.Settings settings) {
        long limit = MV_PULL_BREAKER_LIMIT.get(settings);
        return new org.opensearch.indices.breaker.BreakerSettings(
            "mv_pull",
            limit,
            1.0,
            org.opensearch.core.common.breaker.CircuitBreaker.Type.MEMORY,
            org.opensearch.core.common.breaker.CircuitBreaker.Durability.TRANSIENT,
            null
        );
    }

    @Override
    public void setCircuitBreaker(org.opensearch.core.common.breaker.CircuitBreaker circuitBreaker) {
        this.mvPullBreaker = circuitBreaker;
    }

    @Override
    public void close() throws java.io.IOException {
        // Close the checkpoint replication service before releasing native resources
        MVReplicationService replService = replicationService;
        if (replService != null) {
            replService.close();
        }
        // Stage 2: release the managed DataFusion native runtime
        long ptr = mvNativeRuntimePtr;
        if (ptr != 0) {
            mvNativeRuntimePtr = 0;
            try {
                MVNativeBridge.closeGlobalRuntime(ptr);
            } catch (Exception e) {
                org.apache.logging.log4j.LogManager.getLogger(MVDataFormatPlugin.class)
                    .warn("mv_pull: failed to close managed DataFusion runtime", e);
            }
        }
    }
}
