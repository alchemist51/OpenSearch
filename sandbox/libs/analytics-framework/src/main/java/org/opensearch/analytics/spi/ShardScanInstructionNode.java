/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Instruction node for base shard scan setup — reader acquisition, SessionContext creation,
 * table provider registration. {@code requestsRowIds} signals that the shard scan needs to
 * emit shard-global {@code __row_id__} values (QTF query phase). Inherited by
 * {@link ShardScanWithDelegationInstructionNode} so the same flag applies whether or not
 * filter delegation is in play — QTF and delegation are orthogonal concerns.
 *
 * <p>{@code mvId}/{@code mvStateFingerprint} carry an optional materialized-view rewrite
 * binding (both null when absent). When present, the shard scan handler may split the scan
 * per segment: MV-covered segments read from state files, uncovered from raw — the binding
 * is an <i>option</i> the data node exercises based on snapshot coverage, never an
 * obligation. Carried as optional fields on this node rather than a new
 * {@link InstructionType} so old readers of the instruction list never see an unknown enum
 * ordinal (same wire-evolution concern as the TopK detection note in the Rust bridge).
 * Never set together with delegation (decision D3) or {@code requestsRowIds}.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode, Writeable {

    private final boolean requestsRowIds;
    private final String mvId;
    private final String mvStateFingerprint;

    public ShardScanInstructionNode() {
        this(false);
    }

    public ShardScanInstructionNode(boolean requestsRowIds) {
        this(requestsRowIds, null, null);
    }

    public ShardScanInstructionNode(boolean requestsRowIds, String mvId, String mvStateFingerprint) {
        this.requestsRowIds = requestsRowIds;
        this.mvId = mvId;
        this.mvStateFingerprint = mvStateFingerprint;
    }

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        this.requestsRowIds = in.readBoolean();
        this.mvId = in.readOptionalString();
        this.mvStateFingerprint = in.readOptionalString();
    }

    public boolean requestsRowIds() {
        return requestsRowIds;
    }

    /** True when this scan carries a materialized-view rewrite binding. */
    public boolean hasMVBinding() {
        return mvId != null;
    }

    /** The bound MV id, or null when no binding. */
    public String mvId() {
        return mvId;
    }

    /** Expected state-file schema fingerprint of the bound MV, or null when no binding. */
    public String mvStateFingerprint() {
        return mvStateFingerprint;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(requestsRowIds);
        out.writeOptionalString(mvId);
        out.writeOptionalString(mvStateFingerprint);
    }
}
