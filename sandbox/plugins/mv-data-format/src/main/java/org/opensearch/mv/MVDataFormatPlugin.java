/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.store.checksum.GenericCRC32ChecksumHandler;
import org.opensearch.plugins.ActionPlugin.ActionHandler;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.util.Map;
import java.util.function.Supplier;

/**
 * POC(mv): the materialized-view data format plugin. Registers the derived
 * "materialized_view" format; indices opt in via
 * {@code index.composite.secondary_data_formats: ["materialized_view"]}.
 */
public class MVDataFormatPlugin extends Plugin
    implements
        DataFormatPlugin,
        SearchBackEndPlugin<MVReaderManager.MVReader>,
        org.opensearch.plugins.ClusterPlugin,
        org.opensearch.plugins.ActionPlugin,
        org.opensearch.plugins.ExtensiblePlugin {

    /**
     * Node client captured at component creation; used by the separate-index
     * ship path (POC wiring — production ships over a dedicated transport
     * action with an ack listener rather than a blocking client bulk).
     */
    private volatile org.opensearch.transport.client.Client client;
    private volatile org.opensearch.cluster.service.ClusterService clusterService;

    public MVDataFormatPlugin() {}

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
        // D20/D23/D24: auto-create MV target indices for sources declaring
        // index.mv.views (cluster-manager only; tolerant of re-entry).
        clusterService.addListener(new MVViewsService.TargetCreator(client));
        // NOTE (real-node deployment): all native-using plugins must share ONE
        // native instance — deploy with -Dnative.lib.path pointing at a single
        // .so (SymbolLookup.libraryLookup dlopens the same handle; globals
        // shared; the DataFusion plugin's start() initializes the runtime
        // manager for everyone). Without the property, each classloader
        // extracts its OWN temp copy of the embedded lib = separate globals =
        // "Runtime manager not initialized" from every plugin that didn't
        // init its own. Do NOT init here: double-init corrupts the shared
        // manager.
        return java.util.List.of();
    }

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
            org.opensearch.common.settings.Setting.listSetting(
                MVConstants.VIEWS_SETTING,
                java.util.List.of(),
                java.util.function.Function.identity(),
                org.opensearch.common.settings.Setting.Property.IndexScope
            )
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
        return java.util.List.of(new ActionHandler<>(MVShipStateAction.INSTANCE, MVShipStateTransportHandler.class));
    }

    @Override
    public DataFormat getDataFormat() {
        return MVDataFormat.INSTANCE;
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig config) {
        java.util.List<String> shipTargets = config.indexSettings().getSettings().getAsList(MVConstants.SHIP_TARGETS_SETTING);
        String definition = config.indexSettings().getSettings().get(MVConstants.DEFINITION_SETTING, "payments");
        return new MVIndexingEngine(
            config.store().shardPath(),
            config.indexSettings().getIndex().getName(),
            MVDefinitionSpec.source(definition),
            MVDataFormat.INSTANCE,
            shipTargets == null ? java.util.List.of() : shipTargets,
            () -> client,
            () -> clusterService
        );
    }

    @Override
    public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
        return Map.of(MVDataFormat.NAME, () -> new DataFormatDescriptor(MVDataFormat.NAME, new GenericCRC32ChecksumHandler()));
    }

    @Override
    public void assignCapabilities(MappedFieldType fieldType, IndexSettings indexSettings, DataFormatRegistry dataFormatRegistry) {
        // Derived format: claims no field capabilities ever. Setting an empty
        // map here would clobber assignments made by other formats — so do
        // nothing at all.
    }

    // ---- SearchBackEndPlugin (reader lifecycle for the materialized_view format) ----

    @Override
    public String name() {
        return MVDataFormat.NAME;
    }

    @Override
    public java.util.List<String> getSupportedFormats() {
        return java.util.List.of(MVDataFormat.NAME);
    }

    @Override
    public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
        return new MVReaderManager();
    }
}
