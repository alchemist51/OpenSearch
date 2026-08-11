/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * POC(mv) reader manager: exposes the MV state files of a catalog snapshot.
 * The "reader" is simply the list of state-file sets for the snapshot —
 * coverage IS the snapshot (a segment is covered iff its materialized_view
 * entry exists).
 */
public final class MVReaderManager implements EngineReaderManager<MVReaderManager.MVReader> {

    private static final Logger logger = LogManager.getLogger(MVReaderManager.class);

    /**
     * Snapshot-scoped view of MV state files.
     *
     * <p>Coverage semantics: the writer generation is owned by the composite
     * engine and shared across formats for the same flush, so a source segment
     * is MV-covered iff a state file set with the same {@code writerGeneration}
     * exists in this reader. The read-path coverage split is
     * {@code coveredGenerations()} intersected with the primary format's
     * generations — both taken from the <b>same</b> catalog snapshot, so the
     * split is atomic with the snapshot.
     */
    public record MVReader(List<WriterFileSet> stateFiles) {

        /** Writer generations that have an MV state file in this snapshot. */
        public Set<Long> coveredGenerations() {
            return stateFiles.stream().map(WriterFileSet::writerGeneration).collect(Collectors.toUnmodifiableSet());
        }

        /** State file sets keyed by writer generation. */
        public Map<Long, WriterFileSet> stateFilesByGeneration() {
            return stateFiles.stream().collect(Collectors.toUnmodifiableMap(WriterFileSet::writerGeneration, Function.identity()));
        }

        /** True iff the given writer generation has an MV state file in this snapshot. */
        public boolean covers(long writerGeneration) {
            return stateFiles.stream().anyMatch(s -> s.writerGeneration() == writerGeneration);
        }
    }

    @Override
    public MVReader getReader(CatalogSnapshot catalogSnapshot) {
        return new MVReader(List.copyOf(catalogSnapshot.getSearchableFiles(MVDataFormat.NAME)));
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {
        if (didRefresh && catalogSnapshot != null) {
            logger.debug("mv afterRefresh: {} state file sets", catalogSnapshot.getSearchableFiles(MVDataFormat.NAME).size());
        }
    }

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) {}

    @Override
    public void onFilesAdded(Collection<String> files) {}

    @Override
    public void onFilesDeleted(Collection<String> files) {}

    @Override
    public void close() throws IOException {}
}
