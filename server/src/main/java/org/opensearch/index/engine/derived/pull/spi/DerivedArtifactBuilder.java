/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.IndexShard;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Builds a local derived artifact from a staged snapshot.
 *
 * <p>Format-specific implementations (e.g. MV DataFusion fold, vector index
 * merge) are injected via the {@link DerivedPullFormat} SPI. The generic pull
 * service calls {@link #build} and publishes the result through
 * {@link IndexShard#publishDerivedArtifact}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DerivedArtifactBuilder extends Closeable {

    /**
     * Builds a derived artifact from the files staged in {@code stageDir}.
     *
     * @param snapshot the snapshot that was staged
     * @param stageDir the local directory containing staged files
     * @param shard    the target shard (for generation reservation and
     *                 publishing)
     * @return the build result
     * @throws IOException on build failure
     */
    BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) throws IOException;
}
