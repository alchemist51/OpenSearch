/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.core.index.shard.ShardId;

/**
 * Colocates a separate-index MV target's primaries with the source index's
 * primaries, shard ordinal to shard ordinal (deterministic 1:1 pairing —
 * equal shard counts are validated at MV creation).
 *
 * <p>The target index opts in via {@code index.mv.colocate_with = <source>}.
 * For a target PRIMARY of ordinal i: {@code canAllocate} answers YES only on
 * the node holding the source's ACTIVE primary of ordinal i (THROTTLE while
 * that primary is unassigned — retry rather than wedge); {@code canRemain}
 * answers NO once the source primary has moved, so the standard reactive
 * machinery relocates the target to follow (no custom mover). Replicas are
 * unconstrained: only the write handoff needs primary-primary locality.
 *
 * <p>Unlike {@code ResizeAllocationDecider} (one-shot, initial recovery only),
 * this pairing is PERSISTENT — the decider fires for the target's whole life.
 * Colocation is an optimization for the in-process state handoff, not a
 * correctness requirement: while the pair is split (failover window), the
 * ship path falls back to transport with identical ack semantics. See
 * separate-index/technical-challenges.md §1–§3.
 */
public final class MVColocationAllocationDecider extends AllocationDecider {

    public static final String NAME = "mv_colocation";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation, "cannot allocate");
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return decide(shardRouting, node, allocation, "cannot remain");
    }

    private Decision decide(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation, String verb) {
        if (shardRouting.primary() == false) {
            return allocation.decision(Decision.YES, NAME, "replicas are not colocation-constrained");
        }
        IndexMetadata targetMetadata = allocation.metadata().index(shardRouting.index());
        if (targetMetadata == null) {
            return allocation.decision(Decision.YES, NAME, "no index metadata");
        }
        String sourceIndexName = targetMetadata.getSettings().get(MVConstants.COLOCATE_WITH_SETTING);
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            return allocation.decision(Decision.YES, NAME, "index is not an MV colocation target");
        }
        IndexMetadata sourceMetadata = allocation.metadata().index(sourceIndexName);
        if (sourceMetadata == null) {
            // Source deleted — do not wedge the target's allocation on a
            // dangling pairing; lifecycle handling decides the target's fate.
            return allocation.decision(Decision.YES, NAME, "colocation source [" + sourceIndexName + "] does not exist");
        }
        ShardId sourceShardId = new ShardId(sourceMetadata.getIndex(), shardRouting.id());
        ShardRouting sourcePrimary = allocation.routingNodes().activePrimary(sourceShardId);
        if (sourcePrimary == null || sourcePrimary.currentNodeId() == null) {
            // Source primary unassigned: allow allocation ANYWHERE rather than
            // wait. Waiting creates a circular dependency — the source's own
            // recovery flush ships to this target (ship-before-commit), so a
            // target that waits for the source can deadlock the pair after a
            // joint outage. Availability wins; once the source is active again,
            // canRemain flips NO and reactive following restores colocation.
            return allocation.decision(Decision.YES, NAME, "source primary " + sourceShardId + " is unassigned; not constraining");
        }
        if (sourcePrimary.currentNodeId().equals(node.nodeId())) {
            return allocation.decision(Decision.YES, NAME, "node holds the source primary " + sourceShardId);
        }
        return allocation.decision(
            Decision.NO,
            NAME,
            verb + ": source primary " + sourceShardId + " is on node [" + sourcePrimary.currentNodeId() + "], not [" + node.nodeId() + "]"
        );
    }
}
