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
import java.util.List;
import java.util.Map;

/**
 * Source-pushed checkpoint publication: after a source shard's refresh uploads
 * a new parquet generation to the remote segment store, it pushes an advert
 * (maxSeqNo, primaryTerm, infosVersion, parquet file manifest) to each bound
 * target shard's node. The target's poller consumes the mailbox instead of
 * doing remote-store listing + metadata init on every round.
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
        private final String sourceIndex;
        private final String sourceUuid;
        private final int sourceShard;
        private final long maxSeqNo;
        private final long primaryTerm;
        private final long infosVersion;
        /** Parquet file names in this generation (flat names, as listed in segment metadata). */
        private final List<String> parquetFiles;
        /** Per-file byte sizes (parallel to parquetFiles); -1 if unavailable. */
        private final List<Long> fileSizes;

        public Request(
            String targetIndex,
            int targetShard,
            String sourceIndex,
            String sourceUuid,
            int sourceShard,
            long maxSeqNo,
            long primaryTerm,
            long infosVersion,
            List<String> parquetFiles,
            List<Long> fileSizes
        ) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.sourceIndex = sourceIndex;
            this.sourceUuid = sourceUuid;
            this.sourceShard = sourceShard;
            this.maxSeqNo = maxSeqNo;
            this.primaryTerm = primaryTerm;
            this.infosVersion = infosVersion;
            this.parquetFiles = List.copyOf(parquetFiles);
            this.fileSizes = List.copyOf(fileSizes);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
            this.sourceIndex = in.readString();
            this.sourceUuid = in.readString();
            this.sourceShard = in.readVInt();
            this.maxSeqNo = in.readZLong();
            this.primaryTerm = in.readZLong();
            this.infosVersion = in.readZLong();
            this.parquetFiles = in.readStringList();
            this.fileSizes = in.readList(StreamInput::readZLong);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeString(sourceIndex);
            out.writeString(sourceUuid);
            out.writeVInt(sourceShard);
            out.writeZLong(maxSeqNo);
            out.writeZLong(primaryTerm);
            out.writeZLong(infosVersion);
            out.writeStringCollection(parquetFiles);
            out.writeCollection(fileSizes, StreamOutput::writeZLong);
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

        public String sourceUuid() {
            return sourceUuid;
        }

        public int sourceShard() {
            return sourceShard;
        }

        public long maxSeqNo() {
            return maxSeqNo;
        }

        public long primaryTerm() {
            return primaryTerm;
        }

        public long infosVersion() {
            return infosVersion;
        }

        public List<String> parquetFiles() {
            return parquetFiles;
        }

        public List<Long> fileSizes() {
            return fileSizes;
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
