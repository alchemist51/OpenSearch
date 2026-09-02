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
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexSettingProvider;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The `index.mv.views` UX (decisions 20/23/24): the user declares MVs on the
 * SOURCE index only — a list of {@code definition} or {@code definition:name}
 * entries — and never touches formats, state schemas, or the target index.
 *
 * <ul>
 *   <li>{@link Provider} (an {@link IndexSettingProvider}) expands the views
 *       list at source-creation time into pull-only source storage settings:
 *       Parquet primary plus Lucene secondary. The source publishes ordinary
 *       remote Parquet snapshots and never runs an MV indexing engine or ships
 *       state to targets.</li>
 *   <li>{@link TargetCreator} (a {@link ClusterStateListener}, elected
 *       cluster-manager only) creates each missing target index with the
 *       fully derived settings and mapping (state schema from the definition
 *       spec + the single hidden provenance field of decision 21), colocated
 *       with the source. Tolerates already-exists (listener re-entry).</li>
 * </ul>
 *
 * <p>v1 in-sync-by-construction (decision 24): this path only exists at
 * index creation, so the source is empty by definition. The future
 * add-MV-to-existing-index API must add its own race-free emptiness check.
 *
 * <p>RESOLVED (was "the mapping gap"): the composite plugin's own
 * IndexSettingProvider used to contribute cluster-default formats for every
 * new index; provider iteration order is undefined, so on losing orders its
 * empty secondary list overwrote this provider's pull-only source formats
 * (breaking inline mappings and Parquet publication). The composite provider
 * now defers when `index.mv.views` is declared.
 *
 * <p>Names (decision 23): {@code definition:name} uses {@code name} as the
 * target index; a bare {@code definition} generates
 * {@code <source>_mv_<definition>}. (Dot-prefixed true system indices need
 * SystemIndexPlugin registration — deferred, tracked in the decision log.)
 */
public final class MVViewsService {

    private static final Logger logger = LogManager.getLogger(MVViewsService.class);

    private MVViewsService() {}

    /** Parsed views entry. */
    record View(String definition, String targetIndex) {
    }

    static List<View> parseViews(String sourceIndex, List<String> entries) {
        List<View> views = new ArrayList<>(entries.size());
        for (String entry : entries) {
            int colon = entry.indexOf(':');
            String definition = colon < 0 ? entry : entry.substring(0, colon);
            String name = colon < 0 ? String.format(Locale.ROOT, "%s_mv_%s", sourceIndex, definition) : entry.substring(colon + 1);
            // Fails fast on unknown definitions (registry lookup throws).
            MVDefinitionSpec.source(definition);
            views.add(new View(definition, name));
        }
        return views;
    }

    /** Expands `index.mv.views` into the derived source settings at creation time. */
    public static final class Provider implements IndexSettingProvider {
        @Override
        public Settings getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings templateAndRequestSettings) {
            List<String> entries = templateAndRequestSettings.getAsList(MVConstants.VIEWS_SETTING);
            if (entries.isEmpty()) {
                return Settings.EMPTY;
            }
            List<View> views = parseViews(indexName, entries);
            if (views.stream().map(View::definition).distinct().count() > 1) {
                // POC: one definition setting per source (the registry slot the
                // compiled-PPL definition will replace, decision 22). Multiple
                // DISTINCT definitions need per-target definitions on the ship
                // path — deferred until definitions are real metadata.
                throw new IllegalArgumentException(MVConstants.VIEWS_SETTING + " (POC): all views on one source must share the definition");
            }
            List<String> targets = views.stream().map(View::targetIndex).toList();
            logger.info("[{}] mv views declared: deriving pull-only source settings, targets={}", indexName, targets);
            return Settings.builder()
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene")
                .build();
        }
    }

    /** Creates missing target indices for sources that declare views. */
    public static final class TargetCreator implements ClusterStateListener {

        private final Client client;
        /** Creations already dispatched this node-lifetime (double-fire guard; already-exists is tolerated anyway). */
        private final Set<String> dispatched = ConcurrentHashMap.newKeySet();

        public TargetCreator(Client client) {
            this.client = client;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.localNodeClusterManager() == false || event.metadataChanged() == false) {
                return;
            }
            for (IndexMetadata imd : event.state().metadata().indices().values()) {
                List<String> entries = imd.getSettings().getAsList(MVConstants.VIEWS_SETTING);
                if (entries.isEmpty()) {
                    continue;
                }
                String source = imd.getIndex().getName();
                for (View view : parseViews(source, entries)) {
                    if (event.state().metadata().hasIndex(view.targetIndex())
                        || dispatched.add(source + ">" + view.targetIndex()) == false) {
                        continue;
                    }
                    createTarget(imd, view);
                }
            }
        }

        private void createTarget(IndexMetadata sourceMetadata, View view) {
            String source = sourceMetadata.getIndex().getName();
            int sourceShards = sourceMetadata.getNumberOfShards();

            // Stage 4: compile the definition once and persist its self-contained
            // descriptor (embeds the integrity definition_hash) so the target is
            // resolvable across restarts without the compiledFor() switch. The
            // descriptor is derived from the SAME compiledFor(definition) the
            // target mapping is built from, so definition_id and descriptor agree
            // by construction; validateCreation re-checks that agreement (and the
            // size guard) fail-closed before the create request is submitted.
            MVCompiledDefinition compiledDef = MVCompiledDefinition.compiledFor(view.definition());
            String descriptorJson = MVDefinitionResolver.serialize(compiledDef.toDescriptor());

            // Submit only public settings; MetadataCreateIndexService enriches private binding.
            // The common target contract (source binding, shards/replicas, refresh,
            // derived category, composite parquet+lucene, colocation) is assembled by
            // the shared MVViewCreation helper so the auto-creation path and the
            // Stage 5 REST create path (PUT /_mv/views/{name}) are byte-identical.
            // This (legacy named) path additionally carries the definition id/name for
            // BWC; the REST path is descriptor-only and self-contained.
            Settings targetSettings = MVViewCreation.commonTargetSettings(source, sourceShards)
                .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, view.definition())
                .put(MVConstants.DEFINITION_SETTING, view.definition())
                // Stage 4: persisted self-contained descriptor (authoritative,
                // resolved first by MVDefinitionResolver).
                .put(MVConstants.DESCRIPTOR_SETTING, descriptorJson)
                .putList(MVConstants.STATE_FIELDS_SETTING, MVDefinitionSpec.source(view.definition()).shipFields())
                .build();

            // Fail closed BEFORE submitting: descriptor is parseable, within the
            // size guard, its integrity hash holds, and it agrees with definition_id.
            MVDefinitionResolver.validateCreation(targetSettings);

            CreateIndexRequest request = new CreateIndexRequest(view.targetIndex()).settings(targetSettings)
                .mapping(targetMapping(view.definition()));
            client.admin().indices().create(request, ActionListener.wrap(r -> {
                logger.info("mv views: created target [{}] for source [{}] (definition={})", view.targetIndex(), source, view.definition());
            }, e -> {
                if (e instanceof org.opensearch.ResourceAlreadyExistsException) {
                    return; // listener re-entry / concurrent manager — fine
                }
                // Loud but non-fatal: the ship path fails the source's first
                // refresh with a clear error until the target exists, so data
                // can't silently commit without its MV.
                logger.error("mv views: target [" + view.targetIndex() + "] creation failed for source [" + source + "]", e);
            }));
        }

        /**
         * Derived target mapping generated from the {@link MVCompiledDefinition}
         * via {@link MVMappingGenerator}. Uses stable user-visible aliases,
         * not DataFusion internal names. Adds the hidden {@code _mv_source_generation}
         * provenance field (decision 21). {@code dynamic:false} — the composite
         * apply path cannot do dynamic mapping updates.
         */
        static String targetMapping(String definition) {
            // Single authoritative compiler — same instance shape the pull-side
            // artifact builder uses, so mapping, projection, hash, and SQL agree.
            // Serialization is delegated to the shared MVViewCreation helper so the
            // auto-creation and REST create paths emit an identical mapping.
            MVCompiledDefinition compiledDef = MVCompiledDefinition.compiledFor(definition);
            return MVViewCreation.targetMapping(compiledDef);
        }
    }
}
