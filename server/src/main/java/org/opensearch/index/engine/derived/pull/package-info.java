/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Generic pull-based derived data service.
 *
 * <p>{@link org.opensearch.index.engine.derived.pull.NodeDerivedPullService}
 * manages one {@link org.opensearch.index.engine.derived.pull.DerivedShardPoller}
 * per locally-started eligible primary shard. Format-specific behavior is
 * injected via the SPI in
 * {@link org.opensearch.index.engine.derived.pull.spi}.
 *
 * <p><b>Invariant:</b> No class in this package imports or references any
 * format-specific type (MV, DataFusion, Parquet, SegmentInfos, etc.).
 *
 * @opensearch.experimental
 */
package org.opensearch.index.engine.derived.pull;
