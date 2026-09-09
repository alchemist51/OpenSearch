/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a data format for storing and managing index data, with declared capabilities.
 * Each data format (e.g., Lucene, Parquet) declares what storage and query capabilities it supports.
 * <p>
 * Equality is based on the format {@link #name()} — there should be one {@code DataFormat} instance
 * per unique name. This allows {@code DataFormat} to be used safely as a {@link java.util.Map} key.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class DataFormat {
    /**
     * Returns the unique name of this data format.
     *
     * @return the data format name
     */
    public abstract String name();

    /**
     * Returns the priority of this data format. Higher priority formats are preferred
     * when multiple formats can handle the same field type.
     *
     * @return the priority value
     */
    public abstract long priority();

    /**
     * Returns the set of field type capabilities supported by this data format.
     *
     * @return the supported field type capabilities
     */
    public abstract Set<FieldTypeCapabilities> supportedFields();

    /**
     * Whether this format is exempt from the per-segment cross-format row-count
     * parity checks. Formats that store the same logical rows as other formats
     * (the default) must report equal row counts per segment. A <b>derived</b>
     * format — one whose files are computed from another format's data rather
     * than ingested row-by-row (e.g. a materialized-view aggregate, one row per
     * group) — reports its own row count and is exempt.
     *
     * <p>Exempt formats must never be used as the source of segment-ordinal to
     * leaf mappings or merge-policy row accounting; those always come from a
     * non-exempt (primary) format.
     *
     * @return true if this format's per-segment row count may differ from other formats'
     */
    public boolean exemptFromRowParity() {
        return false;
    }

    /**
     * Whether this format may legally produce NO files for a generation whose
     * primary flushed. False for regular formats — the composite flush contract
     * is files-for-all-formats-or-none, so a missing file set means data loss.
     * Derived formats whose output lives elsewhere return true: a format that
     * ships its state to an external location (e.g. a separate MV index,
     * ship-before-commit) has nothing to register in this index's catalog, and
     * a derived format may also skip a generation it will cover later
     * (skip-and-backfill).
     *
     * @return true if an empty flush result is legal for this format
     */
    public boolean mayEmitNoFiles() {
        return false;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof DataFormat == false) return false;
        return Objects.equals(name(), ((DataFormat) o).name());
    }

    @Override
    public final int hashCode() {
        return Objects.hashCode(name());
    }
}
