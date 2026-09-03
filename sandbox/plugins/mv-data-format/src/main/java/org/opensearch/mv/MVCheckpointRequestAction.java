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
 * Request-driven checkpoint action: target → source. Every poll round where
 * the target's mailbox is empty, it sends a checkpoint request to the source
 * primary carrying the target's current watermark. The source handler reads
 * its local catalog, filters files and noops to the (watermark, advertMax]
 * range, and replies with a scoped {@link MVReplicationCheckpoint}.
 *
 * <p>This is the primary data flow. The push path (mailbox) is a documented
 * latency optimization that may be re-enabled in the future.
 *
 * <p>On RPC failure, the poller returns null and retries on the next round
 * (covered by the poller's existing backoff).
 *
 * <p>Logs: CHECKPOINT_REQUEST on send, CHECKPOINT_REPLY / CHECKPOINT_NOTHING_NEW
 * on response.
 */
public final class MVCheckpointRequestAction extends ActionType<MVCheckpointRequestAction.Response> {

    public static final String NAME = "indices:data/read/derived_state/checkpoint_request";
    public static final MVCheckpointRequestAction INSTANCE = new MVCheckpointRequestAction();

    private MVCheckpointRequestAction() {
        super(NAME, Response::new);
    }

    /**
     * Request from target to source: "give me files and noops above my
     * current watermark for this source shard."
     */
    public static final class Request extends ActionRequest {
        private final String sourceIndex;
        private final int sourceShard;
        private final String targetIndex;
        private final int targetShard;
        /** Target's current watermark — source filters to (watermark, advertMax]. */
        private final long targetWatermark;

        public Request(String sourceIndex, int sourceShard, String targetIndex, int targetShard, long targetWatermark) {
            this.sourceIndex = sourceIndex;
            this.sourceShard = sourceShard;
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.targetWatermark = targetWatermark;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.sourceIndex = in.readString();
            this.sourceShard = in.readVInt();
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
            this.targetWatermark = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(sourceIndex);
            out.writeVInt(sourceShard);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeZLong(targetWatermark);
        }

        public String sourceIndex() { return sourceIndex; }
        public int sourceShard() { return sourceShard; }
        public String targetIndex() { return targetIndex; }
        public int targetShard() { return targetShard; }
        public long targetWatermark() { return targetWatermark; }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /**
     * Response from source: carries an {@link MVReplicationCheckpoint}.
     * {@code available} is false if the source has no new data (nothing-new).
     */
    public static final class Response extends ActionResponse {
        private final boolean available;
        private final MVReplicationCheckpoint checkpoint;

        /** Source has data: wrap a checkpoint. */
        public Response(MVReplicationCheckpoint checkpoint) {
            this.available = true;
            this.checkpoint = checkpoint;
        }

        /** Source has no new data (nothing-new). */
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
