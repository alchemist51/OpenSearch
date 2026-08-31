/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * SPI interfaces for the generic derived-data pull model.
 *
 * <p>These interfaces define the contract between the format-agnostic
 * {@code NodeDerivedPullService} / {@code DerivedShardPoller} (which live
 * in the server module and handle lifecycle, scheduling, and watermark
 * management) and format-specific implementations (e.g. MV state, vector
 * index) that live in their respective sandbox plugins.
 *
 * <p><b>Contract:</b> No class in this package may import or reference any
 * format-specific type ({@code .si}, {@code .parquet}, DataFusion,
 * MVNativeBridge, MVStateArtifactWriter, etc.).
 *
 * @opensearch.experimental
 */
package org.opensearch.index.engine.derived.pull.spi;
