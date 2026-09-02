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
            // Submit only public settings; MetadataCreateIndexService enriches private binding.
            CreateIndexRequest request = new CreateIndexRequest(view.targetIndex()).settings(
                Settings.builder()
                    .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_SOURCE_NAME, source)
                    .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DEFINITION_ID, view.definition())
                    .put("index.number_of_shards", sourceShards)
                    .put("index.number_of_replicas", 0)
                    // Target search visibility is source-refresh-driven. The
                    // ship handler explicitly refreshes after staging one
                    // complete atomic batch; no scheduled target refresh may
                    // publish rows independently of their source checkpoint.
                    .put("index.refresh_interval", "-1")
                    // First-class derived index: only the replication-owned
                    // shard entry point may write. Its translog carries no
                    // operation payloads; it persists only the checkpoint
                    // required to select the newest safe target catalog.
                    // Source parquet + cursor reconciliation is its data
                    // recovery log.
                    .put(MVConstants.DERIVED_INDEX_SETTING, true)
                    .put("index.append_only.enabled", true)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    // Parquet stores the replicated rows; Lucene remains a
                    // physical query-capability projection. The materialized-view
                    // state artifact (mv_state) is NOT a secondary format — it is
                    // owned by the derived category declared below and injected
                    // into the composite store by the category resolution.
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene")
                    // Canonical DERIVED DATA-FORMAT CATEGORY: the single, immutable
                    // declaration that this is a materialized-view derived target.
                    .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DATA_FORMAT, MVDataFormat.NAME)
                    .put(MVConstants.DEFINITION_SETTING, view.definition())
                    .putList(MVConstants.STATE_FIELDS_SETTING, MVDefinitionSpec.source(view.definition()).shipFields())
                    .put(MVConstants.COLOCATE_WITH_SETTING, source)
            ).mapping(targetMapping(view.definition()));
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
            MVCompiledDefinition compiledDef = MVCompiledDefinition.compiledFor(definition);
            MVMappingGenerator generator = new MVMappingGenerator();
            java.util.Map<String, Object> mapping = generator.generateMapping(compiledDef);

            // Serialize to JSON with dynamic:false and provenance field
            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> properties = (java.util.Map<String, Object>) mapping.get("properties");
            // Add provenance field
            java.util.Map<String, Object> provenance = new java.util.LinkedHashMap<>();
            provenance.put("type", "long");
            provenance.put("index", false);
            properties.put("_mv_source_generation", provenance);

            StringBuilder sb = new StringBuilder("{\"dynamic\":\"false\",\"_field_names\":{\"enabled\":false},\"properties\":{");
            boolean first = true;
            for (java.util.Map.Entry<String, Object> entry : properties.entrySet()) {
                if (first == false) {
                    sb.append(",");
                }
                first = false;
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> fieldMap = (java.util.Map<String, Object>) entry.getValue();
                sb.append("\"").append(entry.getKey()).append("\":{");
                boolean firstField = true;
                for (java.util.Map.Entry<String, Object> fe : fieldMap.entrySet()) {
                    if (firstField == false) {
                        sb.append(",");
                    }
                    firstField = false;
                    sb.append("\"").append(fe.getKey()).append("\":");
                    if (fe.getValue() instanceof Boolean) {
                        sb.append(fe.getValue());
                    } else {
                        sb.append("\"").append(fe.getValue()).append("\"");
                    }
                }
                sb.append("}");
            }
            sb.append("}}");
            return sb.toString();
        }
    }
}
