/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.derived.pull.NodeDerivedPullService;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.mv.pull.MVBuildRuntime;
import org.opensearch.mv.pull.MVDerivedPullFormat;
import org.opensearch.mv.pull.MVPullSettings;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Unified materialized-view plugin. Registers both the derived
 * "materialized_view" source format and the "mv_state" target format.
 * Provides the pull-based build service for {@code mv_state} targets
 * through the generic {@link NodeDerivedPullService} SPI.
 *
 * <p>This plugin consolidates the former mv-data-format, mv-pull-engine,
 * and mv-state-format modules into a single deployable unit. The generic
 * SPI interfaces ({@code DerivedPullFormat}, {@code DerivedSourceReader},
 * etc.) remain in the server module; only MV-specific implementations
 * live here.</p>
 *
 * <p><b>One plugin, two formats:</b> {@code materialized_view} is the
 * source-side format (captures aggregates from raw data); {@code mv_state}
 * is the target-side format (folds shipped/pulled state). Both are
 * {@link org.opensearch.index.engine.dataformat.DerivedDataFormat}
 * subclasses.</p>
 */
public class MVDataFormatPlugin extends Plugin
    implements
        DataFormatPlugin,
        SearchBackEndPlugin<MVReaderManager.MVReader>,
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
            org.opensearch.common.settings.Setting.listSetting(
                MVConstants.SHIP_TARGETS_SETTING,
                java.util.List.of(),
                java.util.function.Function.identity(),
                org.opensearch.common.settings.Setting.Property.IndexScope
            ),
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
            org.opensearch.common.settings.Setting.boolSetting(
                MVConstants.STATE_MERGE_SETTING,
                false,
                org.opensearch.common.settings.Setting.Property.IndexScope,
                org.opensearch.common.settings.Setting.Property.Dynamic
            ),
            org.opensearch.common.settings.Setting.boolSetting(
                MVConstants.SERVE_STATE_SETTING,
                false,
                org.opensearch.common.settings.Setting.Property.IndexScope,
                org.opensearch.common.settings.Setting.Property.Dynamic
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
            new ActionHandler<>(MVShipStateAction.INSTANCE, MVShipStateTransportHandler.class),
            new ActionHandler<>(MVCursorAction.INSTANCE, MVCursorTransportHandler.class),
            new ActionHandler<>(MVSourceCommitAction.INSTANCE, MVSourceCommitTransportHandler.class),
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

    @Override
    public DataFormat getDataFormat() {
        return MVDataFormat.INSTANCE;
    }

    @Override
    public java.util.List<DataFormat> getAdditionalDataFormats() {
        return java.util.List.of(MVStateDataFormat.INSTANCE);
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig config) {
        java.util.List<String> shipTargets = config.indexSettings().getSettings().getAsList(MVConstants.SHIP_TARGETS_SETTING);
        // ONE definition carrier: the canonical derived-binding key. No silent
        // default — an MV-participating index without a declared definition is
        // a configuration bug and must fail loudly, not fold a toy definition.
        String definition = config.indexSettings().getSettings().get(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID);
        if (definition == null || definition.isEmpty()) {
            throw new IllegalStateException(
                "mv engine: index ["
                    + config.indexSettings().getIndex().getName()
                    + "] participates in MV but declares no definition — set "
                    + org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID
                    + " (targets created via /_mv/views carry it automatically)"
            );
        }

        // Source vs target is decided by the canonical DERIVED DATA-FORMAT
        // CATEGORY, not by scanning primary/secondary format lists for mv_state.
        // A target declares index.derived.data_format=materialized_view; a
        // source declares MV ship targets (index.mv.ship_targets).
        String derivedCategory = org.opensearch.cluster.metadata.DerivedIndexBinding.dataFormatCategory(
            config.indexSettings().getSettings()
        );
        boolean isMvStateTarget = MVDataFormat.NAME.equals(derivedCategory);

        MVDefinitionSpec spec;
        org.opensearch.index.engine.dataformat.DataFormat format;
        if (isMvStateTarget && (shipTargets == null || shipTargets.isEmpty())) {
            // Target side: use fold definition and mv_state format
            spec = MVDefinitionSpec.fold(definition);
            format = MVStateDataFormat.INSTANCE;
        } else {
            // Source side: use source definition and materialized_view format
            spec = MVDefinitionSpec.source(definition);
            format = MVDataFormat.INSTANCE;
        }

        // Stage 4: resolve the target-side merge definition through the shared
        // MVDefinitionResolver (persisted descriptor first, else legacy
        // compiledFor). A tampered / oversize / unparseable / disagreeing
        // descriptor throws — we fail closed to a null merge definition so the
        // engine disables state merge rather than merging with wrong fold
        // semantics. Only consulted on the target-merge branch.
        MVCompiledDefinition mergeDefinition = null;
        if (isMvStateTarget && (shipTargets == null || shipTargets.isEmpty())) {
            try {
                mergeDefinition = MVDefinitionResolver.resolve(config.indexSettings().getSettings());
            } catch (RuntimeException e) {
                org.apache.logging.log4j.LogManager.getLogger(MVDataFormatPlugin.class)
                    .error(
                        "mv merge: definition resolution failed for target [{}]; state merge disabled for its shards",
                        config.indexSettings().getIndex().getName(),
                        e
                    );
                mergeDefinition = null;
            }
        }

        return new MVIndexingEngine(
            config.store().shardPath(),
            config.indexSettings().getIndex().getName(),
            spec,
            format,
            definition,
            shipTargets == null ? java.util.List.of() : shipTargets,
            () -> client,
            () -> clusterService,
            config.indexSettings().getSettings().getAsBoolean(MVConstants.STATE_MERGE_SETTING, false),
            routingSnapshotService != null ? routingSnapshotService::current : () -> TargetRoutingSnapshot.EMPTY,
            mergeDefinition
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        // Register descriptors for BOTH formats from this single plugin.
        // mv_state uses PrecomputedChecksumStrategy so that CRC32 checksums
        // registered at write time (after native build or compaction) are served
        // in O(1) by the upload/recovery path. Without this, every publish and
        // every restart CRC32-scans the entire mv_state catalog (219 GB @ gen-38).
        return Map.of(
            MVDataFormat.NAME,
            () -> new DataFormatDescriptor(MVDataFormat.NAME, new GenericCRC32ChecksumHandler()),
            MVStateDataFormat.NAME,
            () -> new DataFormatDescriptor(MVStateDataFormat.NAME, new PrecomputedChecksumStrategy())
        );
    }

    @Override
    public void assignCapabilities(MappedFieldType fieldType, IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        // Derived format: claims no field capabilities ever.
    }

    // ---- SearchBackEndPlugin (reader lifecycle for both formats) ----

    @Override
    public String name() {
        return MVDataFormat.NAME;
    }

    @Override
    public java.util.List<String> getSupportedFormats() {
        // Support both materialized_view and mv_state formats
        return java.util.List.of(MVDataFormat.NAME, MVStateDataFormat.NAME);
    }

    @Override
    public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
        return new MVReaderManager();
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
