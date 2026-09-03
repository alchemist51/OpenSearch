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
 * Cold-start request: target → source. When a target shard has an empty
 * mailbox and has NOT been seeded (first round after creation/restart),
 * it sends a request to the source primary asking for a full advert
 * (same payload as a push). This eliminates the expensive remote-store
 * listing on cold start (up to 257s observed in production).
 *
 * <p>Flow: target sends request → source handler reads local catalog
 * snapshot → replies with full {@link MVReplicationCheckpoint} → target
 * delivers into mailbox → proceeds as MAILBOX_HIT.
 *
 * <p>The legacy latestAdvert pull (full remote listing) is used ONLY as
 * a last resort if the RPC fails.
 *
 * <p>Logs: COLD_START_REQUEST on send, COLD_START_REPLY on response.
 */
public final class MVCheckpointRequestAction extends ActionType<MVCheckpointRequestAction.Response> {

    public static final String NAME = "indices:data/read/derived_state/checkpoint_request";
    public static final MVCheckpointRequestAction INSTANCE = new MVCheckpointRequestAction();

    private MVCheckpointRequestAction() {
        super(NAME, Response::new);
    }

    /**
     * Request from target to source: "give me your current catalog snapshot
     * for this source shard so I can bootstrap my mailbox."
     */
    public static final class Request extends ActionRequest {
        private final String sourceIndex;
        private final int sourceShard;
        private final String targetIndex;
        private final int targetShard;

        public Request(String sourceIndex, int sourceShard, String targetIndex, int targetShard) {
            this.sourceIndex = sourceIndex;
            this.sourceShard = sourceShard;
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.sourceShard = in.readVInt();
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeVInt(sourceShard);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
        }

        public String sourceIndex() { return sourceIndex; }
        public int sourceShard() { return sourceShard; }
        public String targetIndex() { return targetIndex; }
        public int targetShard() { return targetShard; }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Response from source: carries an {@link MVReplicationCheckpoint}.
     * {@code available} is false if the source has no data yet.
     */
    public static final class Response extends ActionResponse {
        private final boolean available;
        private final MVReplicationCheckpoint checkpoint;

        /** Source has data: wrap a checkpoint. */
        public Response(MVReplicationCheckpoint checkpoint) {
            this.available = true;
            this.checkpoint = checkpoint;
        }

        /** Source has no data yet. */
        public static Response unavailable() {
            return new Response(false, null);
        }

        private Response(boolean available, MVReplicationCheckpoint checkpoint) {
            this.available = available;
            this.checkpoint = checkpoint;
        }

        public Response(StreamInput in) throws IOException {
            this.available = in.readBoolean();
            if (available) {
                this.checkpoint = new MVReplicationCheckpoint(in);
            } else {
                this.checkpoint = null;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(available);
            if (available && checkpoint != null) {
                checkpoint.writeTo(out);
            }
        }

        public boolean available() { return available; }

        /** Returns the checkpoint, or null if unavailable. */
        public MVReplicationCheckpoint checkpoint() { return checkpoint; }

        // ── Convenience delegators for backward compat in callers ────────
        public long maxSeqNo() { return checkpoint != null ? checkpoint.maxSeqNo() : -1L; }
        public long primaryTerm() { return checkpoint != null ? checkpoint.primaryTerm() : 0L; }
        public long infosVersion() { return checkpoint != null ? checkpoint.infosVersion() : 0L; }
    }
}
