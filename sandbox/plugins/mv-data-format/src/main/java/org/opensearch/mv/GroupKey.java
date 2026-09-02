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
 * <p>The {@code name} is the stable, user-visible output alias (the
 * materialized column name in the target index and state file). The
 * {@code sqlExpression} is the SQL fragment that produces that column from
 * the raw source table in the partial query — for a plain column reference
 * it is simply the quoted {@code name}, but for a derived key (e.g. a
 * 5-minute time bucket {@code CAST("EventTime" AS BIGINT) / 300000}) it is
 * an arbitrary expression. Keeping the expression separate from the output
 * alias lets the partial SQL emit {@code <expr> AS "<name>"} and repeat
 * {@code <expr>} in GROUP BY, while the fold/merge SQL groups by the already
 * materialized {@code "<name>"} column.</p>
 *
 * @param name          logical column name / stable output alias (user-visible)
 * @param columnType    physical storage type in the state file
 * @param osFieldPath   OpenSearch field path in the source mapping
 * @param sqlExpression SQL expression producing the key in the partial query;
 *                      for plain columns this is the quoted {@code name}
 */
public record GroupKey(String name, ColumnType columnType, String osFieldPath, String sqlExpression) {

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
        Objects.requireNonNull(sqlExpression, "sqlExpression");
    }

    /**
     * Backwards-compatible constructor: the SQL expression defaults to the
     * quoted column {@code name} (a plain column reference).
     */
    public GroupKey(String name, ColumnType columnType, String osFieldPath) {
        this(name, columnType, osFieldPath, quote(name));
    }

    /**
     * Convenience factory for a plain column key: the OS field path and SQL
     * expression both default to the column name.
     */
    public static GroupKey of(String name, ColumnType columnType) {
        return new GroupKey(name, columnType, name, quote(name));
    }

    /**
     * Factory for a derived (expression) key: the output {@code name} is the
     * stable alias, {@code sqlExpression} is the SQL producing it, and
     * {@code osFieldPath} points at the underlying source field the
     * expression reads from.
     */
    public static GroupKey ofExpression(String name, ColumnType columnType, String sqlExpression, String osFieldPath) {
        return new GroupKey(name, columnType, osFieldPath, sqlExpression);
    }

    /** True when this key is a plain column reference (expression == quoted name). */
    public boolean isPlainColumn() {
        return sqlExpression.equals(quote(name));
    }

    /** Quoted identifier form used both as the default expression and in SELECT lists. */
    static String quote(String id) {
        return "\"" + id + "\"";
    }
}
