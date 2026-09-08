/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;

import java.util.Objects;

/**
 * Path input for MV state files in remote store. Extends
 * {@link RemoteStorePathStrategy.ShardDataPathInput} to append an {@code mvId}
 * segment between the shard ID and the data category.
 *
 * <p>Resulting path: {@code <base>/<indexUUID>/<shard>/mv_state/<mvId>/{data|metadata}}</p>
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MVStatePathInput extends RemoteStorePathStrategy.ShardDataPathInput {

    private final String mvId;

    public MVStatePathInput(Builder builder) {
        super(builder);
        this.mvId = Objects.requireNonNull(builder.mvId, "mvId is required");
    }

    public String mvId() {
        return mvId;
    }

    @Override
    BlobPath fixedSubPath() {
        // Parent gives: <indexUUID>/<shard>/<category>/<type>
        // We need:      <indexUUID>/<shard>/mv_state/<mvId>/{data|metadata}
        // So override to insert mvId between shard and category.
        // Replicate the parent logic but insert mvId.
        BlobPath path = BlobPath.cleanPath().add(indexUUID()).add(shardId());
        path = path.add(dataCategory().getName());
        path = path.add(mvId);
        path = path.add(dataType().getName());
        return path;
    }

    /**
     * Returns a new builder for {@link MVStatePathInput}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link MVStatePathInput}.
     */
    @ExperimentalApi
    public static class Builder extends RemoteStorePathStrategy.ShardDataPathInput.Builder {
        private String mvId;

        public Builder mvId(String mvId) {
            this.mvId = mvId;
            return this;
        }

        @Override
        public Builder shardId(String shardId) {
            super.shardId(shardId);
            return this;
        }

        @Override
        public Builder dataCategory(DataCategory dataCategory) {
            super.dataCategory(dataCategory);
            return this;
        }

        @Override
        public Builder dataType(DataType dataType) {
            super.dataType(dataType);
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public MVStatePathInput build() {
            return new MVStatePathInput(this);
        }
    }
}
