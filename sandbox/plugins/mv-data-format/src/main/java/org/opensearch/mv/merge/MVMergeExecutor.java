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
import org.opensearch.index.engine.dataformat.Merger;

import java.io.IOException;
import java.util.Objects;

/** Executes MV merges through a pluggable {@link MVMergeStrategy}. */
public final class MVMergeExecutor implements Merger {

    private final MVMergeStrategy strategy;

    public MVMergeExecutor(MVMergeStrategy strategy) {
        this.strategy = Objects.requireNonNull(strategy);
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        return strategy.mergeMVFiles(mergeInput);
    }
}
