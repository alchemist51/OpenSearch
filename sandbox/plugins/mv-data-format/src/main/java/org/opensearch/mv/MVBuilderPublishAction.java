/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.single.shard.SingleShardRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.mv.pull.MVBuilderOutbox;

import java.io.IOException;

/**
 * Builder-shard emulation, decision D1: the leader (builder) pushes one
 * publication to a follower's primary as a binary transport request that
 * already names the state file to fetch. Routed by
 * {@link org.opensearch.action.support.single.shard.TransportSingleShardAction}
 * to the node holding the FOLLOWER primary; the response is the follower's
 * acknowledgement (its applied watermark), which is what trimming keys on.
 */
public final class MVBuilderPublishAction extends ActionType<MVBuilderPublishAction.Response> {

    public static final String NAME = "indices:data/write/derived_state/builder_publish";
    public static final MVBuilderPublishAction INSTANCE = new MVBuilderPublishAction();

    private MVBuilderPublishAction() {
        super(NAME, Response::new);
    }

    /** "Here is publication (from, to] for you: fetch {@code stateBlob} from your outbox and publish it." */
    public static final class Request extends SingleShardRequest<Request> {
        private final int followerShard;
        private final String sourceIndexUuid;
        private final int sourceShard;
        private final MVBuilderOutbox.Publication publication;

        public Request(
            String followerIndex,
            int followerShard,
            String sourceIndexUuid,
            int sourceShard,
            MVBuilderOutbox.Publication publication
        ) {
            super(followerIndex);
            this.followerShard = followerShard;
            this.sourceIndexUuid = sourceIndexUuid;
            this.sourceShard = sourceShard;
            this.publication = publication;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.followerShard = in.readVInt();
            this.sourceIndexUuid = in.readString();
            this.sourceShard = in.readVInt();
            this.publication = MVBuilderOutbox.Publication.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(followerShard);
            out.writeString(sourceIndexUuid);
            out.writeVInt(sourceShard);
            publication.writeTo(out);
        }

        public String followerIndex() {
            return index();
        }

        public int followerShard() {
            return followerShard;
        }

        public String sourceIndexUuid() {
            return sourceIndexUuid;
        }

        public int sourceShard() {
            return sourceShard;
        }

        public MVBuilderOutbox.Publication publication() {
            return publication;
        }

        @Override
        public ActionRequestValidationException validate() {
            return super.validateNonNullIndex();
        }
    }

    /**
     * Acknowledgement. {@code applied} is true when the publication was
     * published (or had already been); {@code appliedWatermark} is the
     * follower's watermark after the call — when it is below the request's
     * {@code from}, the follower is behind and the leader must resend the
     * missing publications first.
     */
    public static final class Response extends ActionResponse {
        private final boolean applied;
        private final long appliedWatermark;
        private final long publishMillis;
        private final String detail;

        public Response(boolean applied, long appliedWatermark, long publishMillis, String detail) {
            this.applied = applied;
            this.appliedWatermark = appliedWatermark;
            this.publishMillis = publishMillis;
            this.detail = detail;
        }

        public Response(StreamInput in) throws IOException {
            this.applied = in.readBoolean();
            this.appliedWatermark = in.readZLong();
            this.publishMillis = in.readVLong();
            this.detail = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(applied);
            out.writeZLong(appliedWatermark);
            out.writeVLong(publishMillis);
            out.writeOptionalString(detail);
        }

        public boolean applied() {
            return applied;
        }

        public long appliedWatermark() {
            return appliedWatermark;
        }

        public long publishMillis() {
            return publishMillis;
        }

        public String detail() {
            return detail;
        }
    }
}
