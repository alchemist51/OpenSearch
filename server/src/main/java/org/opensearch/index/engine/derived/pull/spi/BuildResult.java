/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;

/**
 * Result of building a local derived artifact from a staged snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface BuildResult {

    /**
     * Whether the build succeeded. A failed build is retried on the next
     * poll round without advancing the watermark.
     */
    boolean success();

    /**
     * An opaque identifier for the built artifact (e.g. generation number,
     * file set hash). Used for logging and diagnostics.
     */
    String artifactId();

    /**
     * Optional build statistics (rows processed, bytes written, duration).
     * Never {@code null} — return an empty map if no stats are available.
     */
    Map<String, Object> stats();
}
