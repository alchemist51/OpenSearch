/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;

public class UpdatePitAction extends ActionType<UpdatePitResponse> {
    public static final UpdatePitAction INSTANCE = new UpdatePitAction();
    public static final String NAME = "indices:data/read/point_in_time/update";

    private UpdatePitAction(){
        super(NAME, UpdatePitResponse::new);
    }
}
