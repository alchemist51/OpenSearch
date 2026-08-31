/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Reads a remote derived source into a local staging directory.
 *
 * <p>Format-specific implementations (e.g. MV remote-store reader) are
 * injected via the {@link DerivedPullFormat} SPI. The generic pull service
 * calls these methods and never knows what file types are being staged.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DerivedSourceReader extends Closeable {

    /**
     * Fetches a point-in-time snapshot descriptor from the remote source
     * for the given shard. Returns {@code null} if no new data is available
     * since {@code sinceWatermark}.
     *
     * @param shard          the target shard's routing entry (used to locate
     *                       the corresponding source shard)
     * @param sinceWatermark the watermark of the last successfully applied
     *                       snapshot; only snapshots strictly beyond this
     *                       value are returned
     * @return a snapshot descriptor, or {@code null} if no new data
     * @throws IOException on remote read failure
     */
    DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) throws IOException;

    /**
     * Downloads the data described by {@code snapshot} into {@code stageDir}.
     * The staging directory is created by the caller and cleaned up after
     * the build step completes.
     *
     * @param snapshot the snapshot to materialize
     * @param stageDir the local staging directory
     * @throws IOException on download failure
     */
    void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException;
}
