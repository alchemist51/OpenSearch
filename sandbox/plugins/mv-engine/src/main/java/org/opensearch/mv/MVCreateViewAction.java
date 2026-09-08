/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionType;

/**
 * Validate a definition and create its derived {@code materialized_view} target
 * index. Backs {@code PUT /_mv/views/{name}}. The poller starts automatically
 * via the existing derived-pull lifecycle once the target shard is STARTED.
 */
public final class MVCreateViewAction extends ActionType<MVCreateViewResponse> {

    public static final String NAME = "cluster:admin/mv/views/create";
    public static final MVCreateViewAction INSTANCE = new MVCreateViewAction();

    private MVCreateViewAction() {
        super(NAME, MVCreateViewResponse::new);
    }
}
