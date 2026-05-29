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
import java.util.List;

/**
 * Instruction node for base shard scan setup — reader acquisition, SessionContext creation,
 * table provider registration. {@code requestsRowIds} signals that the shard scan needs to
 * emit shard-global {@code __row_id__} values (QTF query phase). Inherited by
 * {@link ShardScanWithDelegationInstructionNode} so the same flag applies whether or not
 * filter delegation is in play — QTF and delegation are orthogonal concerns.
 *
 * <p>{@code countQuery} signals that the surrounding plan is a {@code count(*)} or
 * {@code count(col)} aggregate over a fully-delegable filter. The data node may attempt
 * a count fast path (skipping engine execution) before falling through to the standard
 * scan. {@code countExistenceFields} carries the column names that must satisfy
 * {@code IS NOT NULL} when the aggregate is {@code count(col)} — empty when only
 * {@code count(*)} appears. Eligibility decision is the planner's; the data node decides
 * whether the fast path actually fires.
 *
 * @opensearch.internal
 */
public class ShardScanInstructionNode implements InstructionNode, Writeable {

    private final boolean requestsRowIds;
    private final boolean countQuery;
    private final List<String> countExistenceFields;

    public ShardScanInstructionNode() {
        this(false, false, List.of());
    }

    public ShardScanInstructionNode(boolean requestsRowIds) {
        this(requestsRowIds, false, List.of());
    }

    public ShardScanInstructionNode(boolean requestsRowIds, boolean countQuery, List<String> countExistenceFields) {
        this.requestsRowIds = requestsRowIds;
        this.countQuery = countQuery;
        this.countExistenceFields = List.copyOf(countExistenceFields);
    }

    public ShardScanInstructionNode(StreamInput in) throws IOException {
        this.requestsRowIds = in.readBoolean();
        this.countQuery = in.readBoolean();
        this.countExistenceFields = in.readStringList();
    }

    public boolean requestsRowIds() {
        return requestsRowIds;
    }

    public boolean countQuery() {
        return countQuery;
    }

    public List<String> countExistenceFields() {
        return countExistenceFields;
    }

    @Override
    public InstructionType type() {
        return InstructionType.SETUP_SHARD_SCAN;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(requestsRowIds);
        out.writeBoolean(countQuery);
        out.writeStringCollection(countExistenceFields);
    }
}
