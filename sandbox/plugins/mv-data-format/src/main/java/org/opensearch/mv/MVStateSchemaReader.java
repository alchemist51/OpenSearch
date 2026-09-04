/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.io.IOException;
import java.util.List;

/**
 * Reader for MV Parquet state-file schemas.
 *
 * <p>Delegates to the native {@code df_mv_state_field_names} (a footer-only
 * Parquet metadata read through the same parquet crate that owns every other
 * state read) instead of shipping a second Parquet footer parser in Java —
 * one reader stack.
 *
 * <p>The field names are PHYSICAL names as DataFusion's Partial aggregate
 * wrote them (e.g. {@code date_bin(...,mv_input.EventTime)}), which is the
 * ground truth the Rust merge derives its ordering identity from.
 *
 * @opensearch.internal
 */
public final class MVStateSchemaReader {

    private MVStateSchemaReader() {}

    /**
     * Read all field names from a Parquet MV state file's footer schema, in
     * physical order.
     *
     * @param path absolute path to the Parquet state file
     * @return field names in schema order
     * @throws IOException if the file cannot be read or is not valid Parquet
     */
    public static List<String> readFieldNames(String path) throws IOException {
        final List<String> names;
        try {
            names = MVNativeBridge.stateFieldNames(path);
        } catch (RuntimeException e) {
            throw new IOException("Failed to read Parquet state schema from " + path, e);
        }
        if (names.isEmpty()) {
            throw new IOException("Parquet state file has no schema fields: " + path);
        }
        return names;
    }

    /**
     * Read the PHYSICAL names of the first {@code numGroupKeys} fields of a
     * Parquet MV state file. Group keys are the schema prefix by the state
     * contract; these are the names the Rust merge uses for its ordering
     * identity.
     *
     * @param path         path to a Parquet state file
     * @param numGroupKeys how many leading fields are group keys
     * @return the first {@code numGroupKeys} field names, in physical order
     * @throws IOException if the file cannot be read or has fewer fields
     */
    public static List<String> readGroupKeyNames(String path, int numGroupKeys) throws IOException {
        List<String> allNames = readFieldNames(path);
        if (allNames.size() < numGroupKeys) {
            throw new IOException(
                "Parquet state file has " + allNames.size() + " fields but " + numGroupKeys + " group keys were expected: " + path
            );
        }
        return List.copyOf(allNames.subList(0, numGroupKeys));
    }
}
