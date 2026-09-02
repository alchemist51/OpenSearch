/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import java.io.IOException;

/**
 * Thrown when the native build produces an ordering identity hash that does not
 * match the Java-side expectation. This is a fail-closed validation: the
 * artifact is rejected before publication to prevent schema-drifted state files
 * from entering the merge path.
 *
 * <p>This exception indicates a contract violation between the Java ordering
 * contract ({@link org.opensearch.mv.MVGroupByOrdering}) and the Rust ordering
 * hash computed during the build.</p>
 */
public class MvBuildOrderingMismatchException extends IOException {

    public MvBuildOrderingMismatchException(String message) {
        super(message);
    }

    public MvBuildOrderingMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
