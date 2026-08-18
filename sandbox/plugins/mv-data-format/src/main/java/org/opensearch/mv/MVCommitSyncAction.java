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
 * Commit sync (decision 25), the second half of ship-before-commit's
 * superset rule: before the SOURCE commits generation N, the TARGET must
 * durably commit a catalog snapshot at least as new as the one the gen-N
 * ship ack reported. Target committed state may lead the source's, never
 * trail it.
 *
 * <p>Same locality contract as {@link MVShipStateAction}: colocation pins
 * the pair to one node, dispatch is in-JVM (NodeClient), and
 * {@link Request#writeTo} throws as the locality tripwire.
 */
public final class MVCommitSyncAction extends ActionType<MVCommitSyncAction.Response> {

    public static final String NAME = "indices:data/write/mv_commit_sync";
    public static final MVCommitSyncAction INSTANCE = new MVCommitSyncAction();

    private MVCommitSyncAction() {
        super(NAME, Response::new);
    }

    /** Commit request for one target shard: commit at least {@code minVersion}. */
    public static class Request extends ActionRequest {
        private final String targetIndex;
        private final int targetShard;
        private final long minVersion;

        public Request(String targetIndex, int targetShard, long minVersion) {
            this.targetIndex = targetIndex;
            this.targetShard = targetShard;
            this.minVersion = minVersion;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            throw new IllegalStateException("mv commit sync is local-only (hard locality rule) — must never deserialize");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // Locality tripwire: serializing this request means the pair is
            // NOT colocated — a bug, not a fallback (hard locality rule).
            throw new IllegalStateException("mv commit sync is local-only (hard locality rule) — must never serialize");
        }

        public String targetIndex() {
            return targetIndex;
        }

        public int targetShard() {
            return targetShard;
        }

        public long minVersion() {
            return minVersion;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }

    /** Ack: the target's committed catalog snapshot version (>= requested min). */
    public static class Response extends ActionResponse {
        private final long committedVersion;

        public Response(long committedVersion) {
            this.committedVersion = committedVersion;
        }

        public Response(StreamInput in) throws IOException {
            this.committedVersion = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(committedVersion);
        }

        public long committedVersion() {
            return committedVersion;
        }
    }
}
