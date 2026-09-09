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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;

import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Maintains an up-to-date {@link TargetRoutingSnapshot} by listening to cluster
 * state changes. Engine callbacks read the snapshot via {@link #current()} which
 * is a lock-free {@link AtomicReference#get()}.
 *
 * <p><strong>Why this is safe</strong></p>
 * <ol>
 *   <li>{@link #clusterChanged(ClusterChangedEvent)} is called on the
 *       cluster-applier thread with the event's state — this is the correct
 *       and safe way to read cluster state from an applier callback.</li>
 *   <li>Engine callbacks (e.g. {@code afterCommit}, {@code beforeRefresh})
 *       only call {@link #current()} which reads the {@link AtomicReference}.
 *       This is lock-free: no blocking, no cluster state query, no risk of
 *       deadlock on the applier thread.</li>
 *   <li>{@link AtomicReference#set(Object)} and {@link AtomicReference#get()}
 *       provide happens-before ordering, guaranteeing that the engine always
 *       sees a fully-constructed, immutable {@link TargetRoutingSnapshot}.</li>
 * </ol>
 *
 * <p>Register this service as a {@link ClusterStateListener} via
 * {@code clusterService.addListener(nodeRoutingSnapshotService)} during plugin
 * component creation. Call {@link #close()} to deregister.
 *
 * @opensearch.experimental
 */
public class NodeRoutingSnapshotService implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(NodeRoutingSnapshotService.class);

    private final AtomicReference<TargetRoutingSnapshot> current = new AtomicReference<>(TargetRoutingSnapshot.EMPTY);
    /** Source index name → list of bound target descriptors. Lock-free via AtomicReference. */
    private final AtomicReference<Map<String, List<BoundTarget>>> sourceToTargets = new AtomicReference<>(Map.of());
    private final String nodeId;
    private volatile ClusterService clusterService;

    /**
     * Describes a target index that is bound to a source via DerivedIndexBinding.
     * Immutable, published atomically alongside the routing snapshot.
     */
    public record BoundTarget(String targetIndex, int targetShards, String sourceUuid) {}

    public NodeRoutingSnapshotService(String nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * Binds this service to a cluster service for listener registration.
     * Called during plugin component creation.
     */
    public void bind(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    /**
     * Called on the cluster-applier thread with the {@link ClusterChangedEvent}.
     * This is SAFE because:
     * <ul>
     *   <li>We ARE on the applier thread, so reading {@code event.state()} is
     *       the correct access pattern.</li>
     *   <li>We extract only the lightweight shard-count map and publish it
     *       atomically for engine callbacks to read without blocking.</li>
     * </ul>
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        Map<String, Integer> shardCounts = new HashMap<>();
        Map<String, List<BoundTarget>> srcToTgt = new HashMap<>();
        for (Map.Entry<String, IndexMetadata> entry : state.metadata().indices().entrySet()) {
            String indexName = entry.getKey();
            IndexMetadata metadata = entry.getValue();
            // Capture shard counts for ALL indices — the engine's ship targets
            // may reference any of them. The map is small (index name → int).
            shardCounts.put(indexName, metadata.getNumberOfShards());

            // Build source→target mapping from DerivedIndexBinding.
            // Target indices declare their source via index.derived.source.name/uuid;
            // we invert that into source→[targets] for checkpoint publishing.
            DerivedIndexBinding binding = metadata.getDerivedIndexBinding();
            if (binding != null) {
                String category = DerivedIndexBinding.dataFormatCategory(metadata.getSettings());
                if (category != null) {
                    BoundTarget bt = new BoundTarget(indexName, metadata.getNumberOfShards(), binding.sourceUuid());
                    srcToTgt.computeIfAbsent(binding.sourceName(), k -> new ArrayList<>()).add(bt);
                }
            }
        }
        // Make inner lists immutable
        Map<String, List<BoundTarget>> immutable = new HashMap<>();
        srcToTgt.forEach((k, v) -> immutable.put(k, Collections.unmodifiableList(v)));

        TargetRoutingSnapshot snap = new TargetRoutingSnapshot(shardCounts, state.version(), nodeId);
        current.set(snap); // atomic publish — lock-free
        sourceToTargets.set(Collections.unmodifiableMap(immutable));
        logger.trace("Updated routing snapshot to version {} with {} indices, {} source→target bindings",
            state.version(), shardCounts.size(), srcToTgt.size());
    }

    /**
     * Returns the current routing snapshot. Called from engine callbacks —
     * this is a lock-free {@link AtomicReference#get()}, no blocking, no
     * cluster state access.
     */
    public TargetRoutingSnapshot current() {
        return current.get();
    }

    /**
     * Returns the current source→targets map. Lock-free read via
     * AtomicReference. Used by the checkpoint publisher in afterRefresh
     * to resolve which target shards to push to for a given source index.
     *
     * @return immutable map of source index name → list of bound targets
     */
    public Map<String, List<BoundTarget>> sourceToTargets() {
        return sourceToTargets.get();
    }

    /**
     * Deregisters this listener from the cluster service. Safe to call
     * concurrently with {@link #current()} — ongoing reads see the last
     * published snapshot.
     */
    public void close() {
        ClusterService cs = clusterService;
        if (cs != null) {
            cs.removeListener(this);
        }
    }
}
