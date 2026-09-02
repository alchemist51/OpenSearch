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
 * Describe an existing MV target index: its source binding, derived category,
 * and persisted descriptor summary. Backs {@code GET /_mv/views/{name}} where
 * {@code name} is the target index. Read-only.
 */
public final class MVGetViewAction extends ActionType<MVGetViewResponse> {

    public static final String NAME = "cluster:admin/mv/views/get";
    public static final MVGetViewAction INSTANCE = new MVGetViewAction();

    private MVGetViewAction() {
        super(NAME, MVGetViewResponse::new);
    }
}
