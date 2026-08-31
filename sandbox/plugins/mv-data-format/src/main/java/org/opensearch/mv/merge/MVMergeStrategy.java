/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.merge;

import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;

import java.io.IOException;

/**
 * Strategy for producing an MV data-format output during a standard
 * data-format merge.
 */
public interface MVMergeStrategy {

    /**
     * Merges the MV files represented by {@code mergeInput}.
     */
    MergeResult mergeMVFiles(MergeInput mergeInput) throws IOException;
}
