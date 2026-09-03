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
 * Source-pushed checkpoint publication: after a source shard's refresh uploads
 * a new parquet generation to the remote segment store, it pushes an
 * {@link MVReplicationCheckpoint} to each bound target shard's node. The target's
 * poller consumes the mailbox instead of doing remote-store listing + metadata
 * init on every round.
 *
 * <p>Unlike {@link MVShipStateAction} which is local-only (carries live Arrow
 * buffers), this action is fully wire-serializable — the source and target
 * primaries may be on different nodes in production (the pull model does not
 * require colocation).
 *
 * <p>Flow: source refresh → checkpoint publish → target mailbox →
 * poller consumes → scoped file download → build. The poller still runs as a
 * liveness backstop but rounds become no-ops unless the mailbox advanced.
 */
public final class MVCheckpointPublishAction extends ActionType<MVCheckpointPublishAction.Response> {

    public static final String NAME = "indices:data/write/derived_state/checkpoint_publish";
    public static final MVCheckpointPublishAction INSTANCE = new MVCheckpointPublishAction();

    private MVCheckpointPublishAction() {
        super(NAME, Response::new);
    }

    /**
     * Advert pushed from the source shard to a target shard after the source's
     * parquet generation is published to the remote segment store.
     */
    public static final class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final String sourceUuid;
        private final MVReplicationCheckpoint checkpoint;

        public Request(
            String targetIndex,
            int targetShard,
            String sourceUuid,
            MVReplicationCheckpoint checkpoint
        ) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.sourceUuid = sourceUuid;
            this.checkpoint = checkpoint;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
            this.sourceUuid = in.readString();
            this.checkpoint = new MVReplicationCheckpoint(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeString(sourceUuid);
            checkpoint.writeTo(out);
        }

        public String targetIndex() {
            return targetIndex;
        }

        public int targetShard() {
            return targetShard;
        }

        public String sourceUuid() {
            return sourceUuid;
        }

        public MVReplicationCheckpoint checkpoint() {
            return checkpoint;
        }

        // ── Convenience delegators for callers that read individual fields ──

        public String sourceIndex() {
            return checkpoint.sourceIndex();
        }

        public int sourceShard() {
            return checkpoint.sourceShard();
        }

        public long maxSeqNo() {
            return checkpoint.maxSeqNo();
        }

        public long primaryTerm() {
            return checkpoint.primaryTerm();
        }

        public long infosVersion() {
            return checkpoint.infosVersion();
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Ack: target received and stored the advert in its mailbox. */
    public static final class Response extends ActionResponse {
        private final boolean accepted;
        private final long targetWatermark;

        public Response(boolean accepted, long targetWatermark) {
            this.accepted = accepted;
            this.targetWatermark = targetWatermark;
        }

        public Response(StreamInput in) throws IOException {
            this.accepted = in.readBoolean();
            this.targetWatermark = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(accepted);
            out.writeZLong(targetWatermark);
        }

        public boolean accepted() {
            return accepted;
        }

        public long targetWatermark() {
            return targetWatermark;
        }
    }
}
