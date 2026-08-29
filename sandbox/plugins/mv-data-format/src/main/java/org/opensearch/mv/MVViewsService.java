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
 *       list at source-creation time into the full derived settings: the
 *       {@code materialized_view} secondary format and the ship-target list.
 *       Merge order is provider &lt; template &lt; request, so a user who
 *       explicitly sets the composite formats keeps their value — with a
 *       loud warning if it omits the MV format.</li>
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
 * empty secondary list overwrote this provider's derived formats (breaking
 * inline mappings deterministically-ish and shard recovery seed-dependently).
 * The composite provider now defers when `index.mv.views` is declared.
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
            Settings explicit = templateAndRequestSettings.filter(k -> k.startsWith("index.composite.") || k.equals("index.mv.definition"));
            if (explicit.hasValue("index.composite.secondary_data_formats")
                && templateAndRequestSettings.getAsList("index.composite.secondary_data_formats").contains("materialized_view") == false) {
                logger.warn(
                    "[{}] {} is set but the explicit secondary_data_formats omit 'materialized_view' — "
                        + "the request wins over derived settings; the MV will NOT be maintained",
                    indexName,
                    MVConstants.VIEWS_SETTING
                );
            }
            List<String> targets = views.stream().map(View::targetIndex).toList();
            logger.info("[{}] mv views declared: deriving source settings, targets={}", indexName, targets);
            return Settings.builder()
                .put("index.pluggable.dataformat.enabled", true)
                .put("index.pluggable.dataformat", "composite")
                .put("index.composite.primary_data_format", "parquet")
                .putList("index.composite.secondary_data_formats", "lucene", "materialized_view")
                .put(MVConstants.DEFINITION_SETTING, views.get(0).definition())
                .putList(MVConstants.SHIP_TARGETS_SETTING, targets)
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
                    createTarget(source, view);
                }
            }
        }

        private void createTarget(String source, View view) {
            CreateIndexRequest request = new CreateIndexRequest(view.targetIndex()).settings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
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
                    // Parquet stores the replicated rows; mv_state folds
                    // them. Lucene remains a physical query-capability
                    // projection, never the recovery or existence authority.
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "mv_state")
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
         * Derived target mapping: the SHIP schema of the definition (state
         * columns as the source ships them) + the single hidden provenance
         * field (decision 21). dynamic:false — the composite apply path
         * cannot do dynamic mapping updates.
         */
        static String targetMapping(String definition) {
            MVDefinitionSpec spec = MVDefinitionSpec.source(definition);
            StringBuilder sb = new StringBuilder("{\"dynamic\":\"false\",\"_field_names\":{\"enabled\":false},\"properties\":{");
            List<String> shipFields = spec.shipFields();
            List<MVDefinitionSpec.Column> columns = MVDefinitionSpec.fold(definition).columns();
            for (int i = 0; i < shipFields.size(); i++) {
                String type = columns.get(i).type() == MVDefinitionSpec.ColumnType.UTF8 ? "keyword" : "long";
                sb.append("\"").append(shipFields.get(i)).append("\":{\"type\":\"").append(type).append("\",\"index\":false},");
            }
            sb.append("\"_mv_source_generation\":{\"type\":\"long\",\"index\":false}");
            sb.append("}}");
            return sb.toString();
        }
    }
}
