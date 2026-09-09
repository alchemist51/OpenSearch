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
        DOUBLE("double"),
        /**
         * Timestamp column type — maps to OpenSearch {@code date} and Arrow
         * {@code Timestamp(Millisecond, None)}. Used by span/date_bin group
         * keys that bucket a date-typed source field into time windows.
         * The state column stores the bucket boundary as a timestamp, not an
         * integer epoch ordinal.
         */
        TIMESTAMP("date");

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

    /**
     * Factory for a date-aware span (time-bucket) key: emits
     * {@code date_bin(INTERVAL '<interval>', "<sourceColumn>")} as the SQL
     * expression. The output type is always {@link ColumnType#TIMESTAMP}
     * because {@code date_bin} returns a Timestamp, not an integer ordinal.
     *
     * @param name            stable output alias (e.g. {@code "event_bucket"})
     * @param intervalMs      bucket width in milliseconds (e.g. 300000 for 5 min)
     * @param sourceColumn    the date-typed source column to bucket
     */
    public static GroupKey ofSpan(String name, long intervalMs, String sourceColumn) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(sourceColumn, "sourceColumn");
        if (intervalMs <= 0) {
            throw new IllegalArgumentException("span interval must be positive, got " + intervalMs);
        }
        String intervalSql = formatIntervalSql(intervalMs);
        String sql = "date_bin(INTERVAL '" + intervalSql + "', \"" + sourceColumn + "\")";
        return new GroupKey(name, ColumnType.TIMESTAMP, sourceColumn, sql);
    }

    /** True when this key was created via {@link #ofSpan}. */
    public boolean isSpanKey() {
        return columnType == ColumnType.TIMESTAMP && sqlExpression.startsWith("date_bin(");
    }

    /**
     * Extract the span interval in milliseconds from a span key's SQL expression.
     * Returns -1 if this is not a span key.
     */
    public long spanIntervalMs() {
        if (isSpanKey() == false) {
            return -1;
        }
        return parseIntervalMs(sqlExpression);
    }

    /**
     * Format an interval in milliseconds to the most human-readable SQL
     * INTERVAL literal (e.g. {@code "5 minutes"}, {@code "1 hours"},
     * {@code "500 milliseconds"}).
     */
    static String formatIntervalSql(long ms) {
        if (ms % (3600_000L) == 0) {
            return (ms / 3600_000L) + " hours";
        }
        if (ms % 60_000L == 0) {
            return (ms / 60_000L) + " minutes";
        }
        if (ms % 1000L == 0) {
            return (ms / 1000L) + " seconds";
        }
        return ms + " milliseconds";
    }

    /**
     * Parse the interval back from the SQL expression produced by
     * {@link #formatIntervalSql}. Returns the interval in milliseconds.
     */
    static long parseIntervalMs(String sql) {
        // Extract the interval string from: date_bin(INTERVAL '<value>', ...)
        int start = sql.indexOf("INTERVAL '") + "INTERVAL '".length();
        int end = sql.indexOf("'", start);
        if (start < "INTERVAL '".length() || end < 0) {
            return -1;
        }
        String interval = sql.substring(start, end).trim();
        String[] parts = interval.split("\\s+", 2);
        if (parts.length != 2) {
            return -1;
        }
        long value = Long.parseLong(parts[0]);
        return switch (parts[1].toLowerCase(java.util.Locale.ROOT)) {
            case "hours", "hour" -> value * 3600_000L;
            case "minutes", "minute" -> value * 60_000L;
            case "seconds", "second" -> value * 1000L;
            case "milliseconds", "millisecond" -> value;
            default -> -1;
        };
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
