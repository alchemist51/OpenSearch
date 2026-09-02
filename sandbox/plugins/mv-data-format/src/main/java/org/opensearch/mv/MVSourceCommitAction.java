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

/** Signals that a source shard commit is durable through an exact checkpoint. */
public final class MVSourceCommitAction extends ActionType<MVSourceCommitAction.Response> {

    public static final String NAME = "indices:admin/mv/source_commit";
    public static final MVSourceCommitAction INSTANCE = new MVSourceCommitAction();

    private MVSourceCommitAction() {
        super(NAME, Response::new);
    }

    /** Transport request carrying committed-checkpoint metadata for source-commit. */
    public static final class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final String sourceIndex;
        private final int sourceShard;
        private final long committedCheckpoint;

        public Request(String targetIndex, int targetShard, String sourceIndex, int sourceShard, long committedCheckpoint) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.sourceIndex = sourceIndex;
            this.sourceShard = sourceShard;
            this.committedCheckpoint = committedCheckpoint;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            targetIndex = in.readString();
            targetShard = in.readVInt();
            sourceIndex = in.readString();
            sourceShard = in.readVInt();
            committedCheckpoint = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeString(sourceIndex);
            out.writeVInt(sourceShard);
            out.writeZLong(committedCheckpoint);
        }

        String targetIndex() {
            return targetIndex;
        }

        int targetShard() {
            return targetShard;
        }

        String sourceIndex() {
            return sourceIndex;
        }

        int sourceShard() {
            return sourceShard;
        }

        long committedCheckpoint() {
            return committedCheckpoint;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Transport response carrying the committed-checkpoint acknowledgement. */
    public static final class Response extends ActionResponse {
        private final long committedCheckpoint;

        Response(long committedCheckpoint) {
            this.committedCheckpoint = committedCheckpoint;
        }

        Response(StreamInput in) throws IOException {
            committedCheckpoint = in.readZLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(committedCheckpoint);
        }

        long committedCheckpoint() {
            return committedCheckpoint;
        }
    }
}
