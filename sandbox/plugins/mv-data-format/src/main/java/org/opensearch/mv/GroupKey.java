/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.Objects;

/**
 * One group-by key column in a compiled MV definition.
 *
 * @param name         logical column name (user-visible, deterministic)
 * @param columnType   physical storage type in the state file
 * @param osFieldPath  OpenSearch field path in the source mapping
 */
public record GroupKey(String name, ColumnType columnType, String osFieldPath) {

    /** Physical column types supported by the MV state format. */
    public enum ColumnType {
        KEYWORD("keyword"),
        LONG("long"),
        INTEGER("integer"),
        DOUBLE("double");

        private final String osType;

        ColumnType(String osType) {
            this.osType = osType;
        }

        /** OpenSearch mapping type string. */
        public String osType() {
            return osType;
        }
    }

    public GroupKey {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(columnType, "columnType");
        Objects.requireNonNull(osFieldPath, "osFieldPath");
    }

    /**
     * Convenience factory: the OS field path defaults to the column name.
     */
    public static GroupKey of(String name, ColumnType columnType) {
        return new GroupKey(name, columnType, name);
    }
}
