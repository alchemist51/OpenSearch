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
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
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
        org.opensearch.plugins.ExtensiblePlugin {

    private volatile org.opensearch.transport.client.Client client;
    private volatile org.opensearch.cluster.service.ClusterService clusterService;
    private volatile org.opensearch.action.support.ActionFilter derivedIndexActionFilter;
    private volatile NodeDerivedPullService pullService;
    private volatile NodeRoutingSnapshotService routingSnapshotService;

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
        MVPullSettings.Services mvServices = new MVPullSettings.Services(clusterService, threadPool, repositoriesServiceSupplier);
        MVDerivedPullFormat mvFormat = new MVDerivedPullFormat(mvServices);
        this.pullService = new NodeDerivedPullService(threadPool, java.util.List.of(mvFormat));
        this.pullService.start();

        return java.util.List.of(pullService);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        NodeDerivedPullService service = pullService;
        if (service == null) {
            throw new IllegalStateException("mv_pull: pull service is not initialized");
        }
        indexModule.addIndexEventListener(service);
    }

    @SuppressWarnings("deprecation") // SOURCE_INDEX registered for BWC only
    @Override
    public java.util.List<org.opensearch.common.settings.Setting<?>> getSettings() {
        return java.util.List.of(
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
            org.opensearch.common.settings.Setting.simpleString(
                MVConstants.DEFINITION_SETTING,
                "payments",
                org.opensearch.common.settings.Setting.Property.IndexScope
            ),
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
            MVPullSettings.DEFINITION_HASH
        );
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
            new ActionHandler<>(MVSourceCommitAction.INSTANCE, MVSourceCommitTransportHandler.class)
        );
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
        String definition = config.indexSettings().getSettings().get(MVConstants.DEFINITION_SETTING, "payments");

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
            routingSnapshotService != null ? routingSnapshotService::current : () -> TargetRoutingSnapshot.EMPTY
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        // Register descriptors for BOTH formats from this single plugin
        return Map.of(
            MVDataFormat.NAME,
            () -> new DataFormatDescriptor(MVDataFormat.NAME, new GenericCRC32ChecksumHandler()),
            MVStateDataFormat.NAME,
            () -> new DataFormatDescriptor(MVStateDataFormat.NAME, new GenericCRC32ChecksumHandler())
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
}
