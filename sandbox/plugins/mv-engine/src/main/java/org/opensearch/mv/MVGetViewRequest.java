/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/** Transport request for {@code GET /_mv/views/{name}} — {@code name} is the target index. */
public class MVGetViewRequest extends ActionRequest {

    private final String name;

    public MVGetViewRequest(String name) {
        this.name = name;
    }

    public MVGetViewRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
    }

    public String name() {
        return name;
    }

    @Override
    public ActionRequestValidationException validate() {
        if (name == null || name.isBlank()) {
            return ValidateActions.addValidationError("view [name] is required", null);
        }
        return null;
    }
}
