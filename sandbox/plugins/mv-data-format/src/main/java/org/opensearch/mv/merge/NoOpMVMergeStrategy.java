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

import java.util.Map;

/** MV strategy for source shipping mode, where merges produce no local MV files. */
public final class NoOpMVMergeStrategy implements MVMergeStrategy {

    @Override
    public MergeResult mergeMVFiles(MergeInput mergeInput) {
        return new MergeResult(Map.of());
    }
}
