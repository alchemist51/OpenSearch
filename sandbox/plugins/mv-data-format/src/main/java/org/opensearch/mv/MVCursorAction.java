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
 * Queries a target shard's live published claim for one source shard. The
 * response carries both the compatibility cursor and exact source sequence
 * coverage used by source-refresh reconciliation.
 */
public final class MVCursorAction extends ActionType<MVCursorAction.Response> {

    public static final String NAME = "indices:admin/mv/cursor";
    public static final MVCursorAction INSTANCE = new MVCursorAction();

    private MVCursorAction() {
        super(NAME, Response::new);
    }

    /** Transport request for cursor-based MV reads. */
    public static class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final String sourceIndex;
        private final int sourceShard;

        public Request(String targetIndex, int targetShard, String sourceIndex, int sourceShard) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.sourceIndex = sourceIndex;
            this.sourceShard = sourceShard;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.targetIndex = in.readString();
            this.targetShard = in.readVInt();
            this.sourceIndex = in.readString();
            this.sourceShard = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(targetIndex);
            out.writeVInt(targetShard);
            out.writeString(sourceIndex);
            out.writeVInt(sourceShard);
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

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Certified cursor; generation -1 / checkpoint -1 = nothing certified yet. */
    public static class Response extends ActionResponse {
        private final long certifiedGeneration;
        private final long checkpoint;
        private final MVSourceSeqCoverage sourceCoverage;

        public Response(long certifiedGeneration, long checkpoint) {
            this(certifiedGeneration, checkpoint, MVSourceSeqCoverage.contiguous(checkpoint));
        }

        public Response(long certifiedGeneration, long checkpoint, MVSourceSeqCoverage sourceCoverage) {
            this.certifiedGeneration = certifiedGeneration;
            this.checkpoint = checkpoint;
            this.sourceCoverage = sourceCoverage;
        }

        public Response(StreamInput in) throws IOException {
            this.certifiedGeneration = in.readZLong();
            this.checkpoint = in.readZLong();
            this.sourceCoverage = MVSourceSeqCoverage.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(certifiedGeneration);
            out.writeZLong(checkpoint);
            sourceCoverage.writeTo(out);
        }

        public long certifiedGeneration() {
            return certifiedGeneration;
        }

        public long checkpoint() {
            return checkpoint;
        }

        public MVSourceSeqCoverage sourceCoverage() {
            return sourceCoverage;
        }
    }
}
