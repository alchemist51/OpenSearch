/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

/**
 * Machine-readable reason codes surfaced by {@code POST /_mv/_validate} and
 * {@code PUT /_mv/views/{name}} on rejection. Stable strings so clients can
 * branch on them.
 *
 * <p>Schema-shape rejections discovered by the analytics-engine
 * {@code MVShapeMatcher} (JOIN, WINDOW, UNSUPPORTED_AGG, …) are surfaced by the
 * (future) query-text path and reuse that enum's names directly; the codes here
 * cover the descriptor + creation control plane.
 */
public final class MVValidationReasons {

    private MVValidationReasons() {}

    /** The requested source index does not exist in cluster state. */
    public static final String SOURCE_INDEX_NOT_FOUND = "SOURCE_INDEX_NOT_FOUND";

    /** The submitted descriptor JSON is malformed / structurally invalid. */
    public static final String DESCRIPTOR_PARSE_FAILED = "DESCRIPTOR_PARSE_FAILED";

    /** The descriptor parsed but failed to compile (e.g. integrity-hash mismatch). */
    public static final String DESCRIPTOR_COMPILE_FAILED = "DESCRIPTOR_COMPILE_FAILED";

    /** The native planner rejected the definition outright (unknown column, unparseable SQL, …). */
    public static final String NATIVE_VALIDATION_REJECTED = "NATIVE_VALIDATION_REJECTED";

    /** The definition compiled and planned, but its physical state schema disagrees with the descriptor. */
    public static final String SCHEMA_MISMATCH = "SCHEMA_MISMATCH";

    /** PPL/SQL query-text input is not plannable in this module (requires the analytics-engine planner). */
    public static final String QUERY_TEXT_PLANNING_UNAVAILABLE = "QUERY_TEXT_PLANNING_UNAVAILABLE";

    /** The create request's fail-closed pre-submit validation ({@link MVDefinitionResolver#validateCreation}) failed. */
    public static final String CREATION_VALIDATION_FAILED = "CREATION_VALIDATION_FAILED";

    /** The requested view/target index does not exist (GET). */
    public static final String VIEW_NOT_FOUND = "VIEW_NOT_FOUND";

    /** An internal control-flow exception carrying a reason code and message. */
    public static final class ReasonedException extends RuntimeException {
        private final String reasonCode;

        public ReasonedException(String reasonCode, String message) {
            super(message);
            this.reasonCode = reasonCode;
        }

        public String reasonCode() {
            return reasonCode;
        }
    }
}
