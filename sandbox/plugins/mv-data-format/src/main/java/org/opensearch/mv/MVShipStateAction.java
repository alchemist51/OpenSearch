/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Shard-addressed derived-state replication action. It carries one source
 * generation's Arrow state batch to the ordinal-paired derived target primary.
 * The target applies it through {@code DerivedIndexEngine}'s replication-only
 * capability; no user bulk/index API can invoke that write path.
 *
 * <p>Why a dedicated transport action instead of a client bulk:
 * <ul>
 *   <li><b>Shard-addressed</b>: bulk routes by doc-id hash and sprays one
 *       source shard's rows across ALL target shards, defeating the ordinal
 *       pairing the colocation decider maintains. This action ships shard i's
 *       state to target shard i by construction.</li>
 *   <li><b>Local short-circuit</b>: {@code TransportService} invokes the
 *       handler directly (no serialization) when the resolved node is local —
 *       colocation makes that the common case.</li>
 * </ul>
 */
public final class MVShipStateAction extends ActionType<MVShipStateAction.Response> {

    public static final String NAME = "indices:data/write/derived_state/replicate";
    public static final MVShipStateAction INSTANCE = new MVShipStateAction();

    private MVShipStateAction() {
        super(NAME, Response::new);
    }

    /**
     * One source generation's state batch for one target shard, carried as the
     * LIVE ARROW ROOT — the same buffers the native writer finalized into,
     * zero copies since. Legal because this action is LOCAL-ONLY by the hard
     * locality rule (the handler runs in the same JVM via NodeClient dispatch);
     * wire serialization is deliberately unsupported and loudly fails if the
     * rule is ever broken.
     *
     * <p>Ownership: the request carries a REFERENCE on a shared
     * {@link MVRefCountedStateBatch} (the same batch may be in flight to
     * multiple targets); the handler releases exactly its own reference in
     * its finally, success or failure — never closing the batch under other
     * consumers. The last release, wherever it happens, frees the native
     * memory.
     */
    public static class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final String sourceIndex;
        private final int sourceShard;
        private final long writerGeneration;
        /** Contiguous fold checkpoint at build time (D30 I1); -1 = tracker unavailable. */
        private final long foldCheckpoint;
        /** Max seq-no folded into this batch; -1 = unknown. */
        private final long maxSeqNo;
        /** Definition fingerprint (index.mv.definition name; D30 I6); null = unchecked. */
        private final String definition;
        /** Stable logical identity override; null uses the source generation. */
        private final String batchIdentity;
        /** Exact source operations represented by this chunk. */
        private final MVSourceSeqCoverage sourceCoverage;
        private final java.util.List<String> shipFields;
        private final MVRefCountedStateBatch stateBatch;

        public Request(
            String targetIndex,
            int targetShard,
            String sourceIndex,
            int sourceShard,
            long writerGeneration,
            long foldCheckpoint,
            long maxSeqNo,
            String definition,
            String batchIdentity,
            MVSourceSeqCoverage sourceCoverage,
            java.util.List<String> shipFields,
            MVRefCountedStateBatch stateBatch
        ) {
            this.shipFields = shipFields;
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.sourceIndex = sourceIndex;
            this.sourceShard = sourceShard;
            this.writerGeneration = writerGeneration;
            this.foldCheckpoint = foldCheckpoint;
            this.maxSeqNo = maxSeqNo;
            this.definition = definition;
            this.batchIdentity = batchIdentity;
            this.sourceCoverage = java.util.Objects.requireNonNull(sourceCoverage);
            this.stateBatch = stateBatch;
        }

        public Request(StreamInput in) throws IOException {
            throw new UnsupportedOperationException(
                "mv_ship_state is local-only (hard locality rule): the request carries live Arrow buffers "
                    + "and must never cross a wire — receiving it remotely means the rule was broken"
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException(
                "mv_ship_state is local-only (hard locality rule): the request carries live Arrow buffers and cannot be serialized"
            );
        }

        public String targetIndex() {
            return targetIndex;
        }

        public int targetShard() {
            return targetShard;
        }

        public String sourceIndex() {
            return sourceIndex;
        }

        public int sourceShard() {
            return sourceShard;
        }

        public long writerGeneration() {
            return writerGeneration;
        }

        public long foldCheckpoint() {
            return foldCheckpoint;
        }

        public long maxSeqNo() {
            return maxSeqNo;
        }

        public String definition() {
            return definition;
        }

        public String batchIdentity() {
            return batchIdentity;
        }

        public MVSourceSeqCoverage sourceCoverage() {
            return sourceCoverage;
        }

        public java.util.List<String> shipFields() {
            return shipFields;
        }

        public MVRefCountedStateBatch stateBatch() {
            return stateBatch;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Ack: rows are applied and searchable on the derived target primary. */
    public static class Response extends ActionResponse {
        private final int applied;

        public Response(int applied) {
            this.applied = applied;
        }

        public Response(StreamInput in) throws IOException {
            this.applied = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(applied);
        }

        public int applied() {
            return applied;
        }
    }
}
