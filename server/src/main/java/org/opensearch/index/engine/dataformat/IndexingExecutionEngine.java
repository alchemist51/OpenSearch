/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.store.FormatChecksumStrategy;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Engine for executing indexing operations for a specific data format.
 * Provides writer creation, merging, refresh, and file management capabilities.
 *
 * @param <T> the data format type
 * @param <P> the document input type
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexingExecutionEngine<T extends DataFormat, P extends DocumentInput<?>> extends Closeable {
    /**
     * Creates a new writer for the given writer generation.
     *
     * @param config the writer configuration
     * @return a new writer instance
     */
    Writer<P> createWriter(WriterConfig config);

    /**
     * Returns the merger for combining writer file sets.
     *
     * @return the merger instance
     */
    Merger getMerger();

    /**
     * Performs a refresh operation to make recently written data searchable.
     *
     * @param refreshInput the input containing segments and writer files to refresh
     * @return the refresh result containing refreshed segments
     * @throws IOException if an I/O error occurs during refresh
     */
    RefreshResult refresh(RefreshInput refreshInput) throws IOException;

    /**
     * Returns the next writer generation number to be used when creating a new writer.
     * Each writer is associated with a monotonically increasing generation number
     * that uniquely identifies it within this engine's lifecycle.
     *
     * @return the next writer generation number
     */
    long getNextWriterGeneration();

    /**
     * Returns the data format handled by this engine.
     *
     * @return the data format
     */
    T getDataFormat();

    /**
     * Returns the amount of JVM heap memory used by this engine's indexing buffers.
     *
     * @return heap memory usage in bytes
     */
    long getHeapBytesUsed();

    /**
     * Returns the amount of native (off-heap) memory used by this engine.
     *
     * @return native memory usage in bytes
     */
    long getNativeBytesUsed();

    /**
     * Deletes the specified files grouped by directory.
     *
     * @param filesToDelete map of data format name to collections of file names to delete
     * @return map of data format name to collection of file names that failed to delete
     * @throws IOException if an I/O error occurs during deletion
     */
    Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException;

    /**
     * Creates a new empty document input for this engine's data format.
     *
     * @return a new document input instance
     */
    P newDocumentInput();

    /**
     * Returns the {@link IndexStoreProvider} for this engine, giving search backends
     * access to the shard's {@link org.opensearch.index.store.Store} for opening readers.
     * <p>
     * Engines that do not manage a store (e.g., Parquet) may return {@code null}.
     *
     * @return the store provider, or null if this engine does not expose one
     */
    IndexStoreProvider getProvider();

    /**
     * Called for every owning-engine refresh, even when there are no active
     * writers to flush. Derived followers use this visibility rendezvous to
     * reconcile a target that fell back to its committed cursor after restart.
     *
     * @throws IOException to fail the refresh and preserve visibility ordering
     */
    default void beforeRefresh() throws IOException {}

    /**
     * Called by the engine inside its commit section, after the pre-commit
     * refresh and before commit data is written. A format engine may
     * contribute namespaced commit user data, such as a derived target's
     * source cursor. Throwing refuses this index's commit.
     *
     * @return extra commit user-data entries; empty map for none
     * @throws IOException to refuse the commit
     */
    default java.util.Map<String, String> beforeCommit() throws IOException {
        return java.util.Map.of();
    }

    /**
     * Called before commit with the exact local checkpoint that will be stored
     * in commit metadata. The default delegates to the legacy hook.
     *
     * @param committedLocalCheckpoint local checkpoint selected for this commit
     * @return extra commit user-data entries; empty map for none
     * @throws IOException to refuse the commit
     */
    default java.util.Map<String, String> beforeCommit(long committedLocalCheckpoint) throws IOException {
        return beforeCommit();
    }

    /**
     * Called after this engine's catalog commit succeeds. Formats use this to
     * publish in-memory state that must not be observable as durable before
     * the commit (for example, a derived target cursor).
     */
    default void afterCommit() {}

    /**
     * Called after commit with the exact local checkpoint stored in that
     * commit. The default preserves compatibility with format engines that
     * only implement the parameterless hook.
     */
    default void afterCommit(long committedLocalCheckpoint) {
        afterCommit();
    }

    /**
     * Returns whether the current searchable catalog is eligible to become a
     * durable commit. Returning false cleanly defers the commit without
     * failing the engine; the owning flush still completes its refresh work.
     */
    default boolean commitReady() {
        return true;
    }

    /**
     * Returns whether every segment in a proposed merge is eligible for this
     * format. Derived formats use this to prevent either background or
     * refresh-time merges from incorporating state that is searchable but not
     * yet certified durable.
     *
     * @param segments proposed merge inputs
     * @return true when the merge may proceed
     */
    default boolean isMergeEligible(java.util.List<org.opensearch.index.engine.exec.Segment> segments) {
        return true;
    }

    /**
     * Called once when the owning engine opens, after sequence-number state
     * is recovered from the last commit. A derived source format uses this to
     * seed its fold checkpoint tracker: operations at or below
     * {@code localCheckpoint} are represented by authoritative committed
     * source data and do not replay through the normal writer path.
     *
     * @param maxSeqNo        max sequence number from the last commit
     * @param localCheckpoint local checkpoint from the last commit
     */
    default void onEngineOpen(long maxSeqNo, long localCheckpoint) {}

    /**
     * Called when the owning engine records a no-op for a sequence number
     * (for example, a failed indexing operation that consumed a sequence
     * number in the translog). Derived formats must include it in exact source
     * coverage or the contiguous floor stalls at the first failed operation.
     *
     * @param seqNo the no-op sequence number
     */
    default void onNoOp(long seqNo) {}

    /**
     * Provides the format engine a handle to force a translog sync. A derived
     * source calls it before publishing a state or coverage-only batch, so
     * every claimed operation is durable in the authoritative source first.
     *
     * @param translogSync runnable that synchronously fsyncs the translog
     */
    default void bindTranslogSync(java.util.concurrent.Callable<Void> translogSync) {}

    /**
     * Returns whether a committed catalog snapshot must remain retained for a
     * format-owned recovery baseline. The writable engine consults this hook
     * in addition to normal safe-commit and snapshot holds.
     *
     * @param snapshot committed snapshot considered for deletion
     * @return true to retain the snapshot and all files it references
     */
    default boolean retainCatalogSnapshot(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {
        return false;
    }

    /**
     * Returns the checksum strategy used by this engine, if any. Engines that
     * pre-compute checksums during write expose them through this strategy.
     *
     * @return the checksum strategy, or {@code null}
     */
    default FormatChecksumStrategy getChecksumStrategy() {
        return null;
    }

    /**
     * Provides access to the owning engine's current catalog. Source-refresh
     * reconciliation queries this authoritative file set instead of physical
     * directories, so merged outputs and unreferenced inputs are never both
     * replayed.
     */
    default void bindCatalogSnapshotSupplier(
        java.util.function.Supplier<
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot>> catalogSnapshotSupplier
    ) {}

    default Map<DataFormat, EngineReaderManager<?>> buildReaderManager(ReaderManagerConfig config) throws IOException {
        return config.registry().getReaderManager(config);
    }

    /**
     * Returns the tragic exception recorded by the underlying writer/store, if any.
     * Composite engines multiplex this across delegates and surface the first non-null
     * result so DFAE can fail the engine without consulting the committer.
     *
     * @return the tragic exception, or {@code null} if the engine has not turned tragic
     */
    default Exception getTragicException() {
        return null;
    }

    /**
     * Returns the maximum number of documents this engine can index per shard.
     * Used by {@link org.opensearch.index.engine.DocumentCountTracker} to enforce
     * the document count limit. Defaults to {@link Long#MAX_VALUE} (unlimited).
     */
    default long maxIndexableDocs() {
        return Long.MAX_VALUE;
    }
}
