/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Materialized-view (MV) plan analysis. {@link org.opensearch.analytics.planner.mv.MVShapeMatcher}
 * is a QTF-style post-CBO shape matcher that decides whether a PPL/SQL-derived Calcite plan can
 * become an MV and, on a match, emits an {@code MVDefinitionDescriptor}-shaped
 * {@link org.opensearch.analytics.planner.mv.MVShapeResult}.
 *
 * @opensearch.internal
 */
package org.opensearch.analytics.planner.mv;
