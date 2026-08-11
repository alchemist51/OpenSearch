/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Materialized-view transparent rewrite: the planner-facing MV registry
 * ({@link org.opensearch.analytics.planner.mv.MVRegistry}), the canonical
 * definition form ({@link org.opensearch.analytics.planner.mv.MVDefinition}),
 * and the match+annotate phase
 * ({@link org.opensearch.analytics.planner.mv.MVRewritePhase}) that records
 * {@link org.opensearch.analytics.planner.mv.MVRewriteAnnotation}s in the
 * {@code PlannerContext} side-channel for the DAG layer to bind to shard
 * fragments.
 */
package org.opensearch.analytics.planner.mv;
