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

/**
 * POC(mv) reader manager: exposes the MV state files of a catalog snapshot.
 * The "reader" is simply the list of state-file sets for the snapshot —
 * coverage IS the snapshot (a segment is covered iff its materialized_view
 * entry exists).
 */
public final class MVReaderManager implements EngineReaderManager<MVReaderManager.MVReader> {

    private static final Logger logger = LogManager.getLogger(MVReaderManager.class);

    /** Snapshot-scoped view of MV state files. */
    public record MVReader(List<WriterFileSet> stateFiles) {
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
