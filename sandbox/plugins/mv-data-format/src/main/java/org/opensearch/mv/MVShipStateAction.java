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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * POC(mv) shard-addressed state ship: carries one source generation's state
 * rows to the node holding the ordinal-paired TARGET primary, which applies
 * them through the shard's write path (translog — the ack means durable).
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

    public static final String NAME = "indices:data/write/mv_ship_state";
    public static final MVShipStateAction INSTANCE = new MVShipStateAction();

    private MVShipStateAction() {
        super(NAME, Response::new);
    }

    /** One source generation's state rows for one target shard. */
    public static class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final List<String> docIds;
        private final List<Map<String, Object>> docs;

        public Request(String targetIndex, int targetShard, List<String> docIds, List<Map<String, Object>> docs) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.docIds = docIds;
            this.docs = docs;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
            int n = in.readVInt();
            this.docIds = new ArrayList<>(n);
            this.docs = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                docIds.add(in.readString());
                docs.add(in.readMap());
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeVInt(docIds.size());
            for (int i = 0; i < docIds.size(); i++) {
                out.writeString(docIds.get(i));
                out.writeMap(docs.get(i));
            }
        }

        public String targetIndex() {
            return targetIndex;
        }

        public int targetShard() {
            return targetShard;
        }

        public List<String> docIds() {
            return docIds;
        }

        public List<Map<String, Object>> docs() {
            return docs;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Ack: rows are durably applied on the target primary. */
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
