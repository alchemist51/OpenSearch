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
 * Dry-run compile + validate an MV definition against a source index's real
 * mapping. Backs {@code POST /_mv/_validate}. Read-only: never creates,
 * mutates, or deletes any index.
 *
 * <p>Cluster-admin scope: the action reads cluster-state mappings for the
 * requested source index and plans (but never executes) the definition through
 * the native cross-check.
 */
public final class MVValidateAction extends ActionType<MVValidateResponse> {

    public static final String NAME = "cluster:admin/mv/validate";
    public static final MVValidateAction INSTANCE = new MVValidateAction();

    private MVValidateAction() {
        super(NAME, MVValidateResponse::new);
    }
}
