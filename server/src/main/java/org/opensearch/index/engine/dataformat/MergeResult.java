/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Map;
import java.util.Optional;

/**
 * Result of a merge operation containing merged writer file sets.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MergeResult {

    private final Map<DataFormat, WriterFileSet> mergedWriterFileSet;
    private final RowIdMapping rowIdMapping;
    private final boolean foldsRows;

    /**
     * Constructs a merge result with the given merged writer file sets.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        this(mergedWriterFileSet, null, false);
    }

    /**
     * Constructs a merge result with the given merged writer file sets and row ID mapping.
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @param rowIdMapping the row ID mapping produced during the merge
     */
    public MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping) {
        this(mergedWriterFileSet, rowIdMapping, false);
    }

    private MergeResult(Map<DataFormat, WriterFileSet> mergedWriterFileSet, RowIdMapping rowIdMapping, boolean foldsRows) {
        this.mergedWriterFileSet = mergedWriterFileSet;
        this.rowIdMapping = rowIdMapping;
        this.foldsRows = foldsRows;
    }

    /**
     * A merge that FOLDS rows by design — a definition-aware compaction of
     * partial-aggregate state combines the rows of one group key into one — so
     * its output may (and normally does) hold fewer rows than its inputs. The
     * catalog skips the row-count conservation check for such a result (it
     * still refuses an output with more rows than its inputs).
     *
     * @param mergedWriterFileSet map of data formats to merged writer file sets
     * @return the folding merge result
     */
    public static MergeResult folding(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        return new MergeResult(mergedWriterFileSet, null, true);
    }

    /**
     * Whether this merge folds rows (see {@link #folding(Map)}).
     *
     * @return {@code true} when the output may legitimately hold fewer rows than the inputs
     */
    public boolean foldsRows() {
        return foldsRows;
    }

    /**
     * Gets all merged writer file sets.
     *
     * @return map of data formats to merged writer file sets
     */
    public Map<DataFormat, WriterFileSet> getMergedWriterFileSet() {
        return mergedWriterFileSet;
    }

    /**
     * Gets the merged writer file set for a specific data format.
     *
     * @param dataFormat the data format
     * @return the merged writer file set for the specified format
     */
    public WriterFileSet getMergedWriterFileSetForDataformat(DataFormat dataFormat) {
        return mergedWriterFileSet.get(dataFormat);
    }

    /**
     * Gets the row id mapping.
     *
     * @return the row id mapping
     */
    public Optional<RowIdMapping> rowIdMapping() {
        return Optional.ofNullable(rowIdMapping);
    }
}
