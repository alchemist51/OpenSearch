/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.VersionType;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.SourceToParse;

import java.io.IOException;

/**
 * A writable catalog engine whose contents are derived from another index.
 *
 * <p>A derived index is not a second user-ingest surface. Its only accepted
 * writes run inside {@link #executeReplicationWrite(IOCallable)}, entered by
 * the shard's derived-state replication API. Ordinary index, bulk, update,
 * delete, and dynamic-document writes are rejected by capability rather than
 * by convention.
 *
 * <p>The engine intentionally has no operation-bearing translog. The authoritative
 * source index and its committed, sequence-number-addressable data are the
 * recovery log; the target writes no operation payloads to its translog. Its
 * empty translog persists only the global checkpoint needed to select the
 * newest safe catalog commit, which contains the certified source cursor.
 * Losing an uncommitted target refresh therefore causes bounded catch-up, not
 * data loss at the system-of-record boundary.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DerivedIndexEngine extends DataFormatAwareEngine {

    /** Generic index setting used by DataFormatAwareIndexerFactory. */
    public static final String DERIVED_INDEX_SETTING = "index.derived.enabled";

    private final ThreadLocal<Boolean> replicationWrite = ThreadLocal.withInitial(() -> false);

    public DerivedIndexEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected boolean usesNoOpTranslog() {
        return true;
    }

    /**
     * Background merges are admitted exactly when the target declares its
     * state-row sort contract as a standard index sort ({@code index.sort.*},
     * stamped by the MV control plane from the definition's group-by
     * ordering). For such targets the generic data-format merge path is
     * sort-preserving end-to-end — per-file sorted scans feed a
     * sort-preserving merge and outputs re-stamp footer {@code SortingColumn}
     * metadata — so merged state keeps the sorted-scan advertisement and the
     * fold's ordering assumptions. Without the declared contract the stock
     * merger cannot know the ordering and a merge would silently break both,
     * so those targets keep the engine-level veto — deliberately a structural
     * property of the declared contract, not a setting. Source indices are
     * unaffected.
     */
    @Override
    protected boolean backgroundMergesEnabled() {
        return config().getIndexSettings().getIndexSortConfig().hasIndexSort();
    }

    /**
     * Derived targets adopt whole state artifacts; they never hold doc-level
     * operations, so there is no version map to restore at engine creation.
     * The durable recovery cursor is the certified source watermark in commit
     * userData, not a per-document {@code _seq_no} — and the adopted state
     * files carry no {@code _seq_no} column, so the doc-level restore scan
     * would fail on their schema (it was only ever a no-op before state
     * artifacts shared the primary format's fileset).
     */
    @Override
    protected boolean restoresVersionMapFromDocuments() {
        return false;
    }

    /**
     * Executes one target-side apply through the normal sequence-number,
     * listener, writer, refresh, and catalog machinery while granting the
     * otherwise unavailable derived-write capability to the current thread.
     */
    public <T> T executeReplicationWrite(IOCallable<T> operation) throws IOException {
        if (replicationWrite.get()) {
            return operation.call();
        }
        replicationWrite.set(true);
        try {
            return operation.call();
        } finally {
            replicationWrite.remove();
        }
    }

    /**
     * Executes one derived-state batch ATOMICALLY relative to refresh: the
     * whole batch applies inside the engine's refresh-exclusion bracket
     * (same lock order as refresh/flush), so a published or committed
     * generation can never contain part of a batch — the partial-capture
     * race is structurally impossible. Per-operation indexing inside the
     * batch still runs the normal sequence-number, version-map, and writer
     * machinery (deterministic-id dedup preserved for idempotent re-ships).
     */
    public <T> T executeReplicationBatch(IOCallable<T> batch) throws IOException {
        try {
            return executeReplicationWrite(() -> runUnderRefreshExclusion(batch::call));
        } catch (RuntimeException | IOException e) {
            // A non-idempotent failure may occur after earlier rows were
            // admitted to one or more format writers. Closing the engine is
            // the rollback boundary: no later refresh can publish that
            // unclaimed prefix, and source-driven recovery replays the batch.
            failEngine("derived-state replication batch failed", e);
            throw e;
        }
    }

    @Override
    public Engine.IndexResult index(Engine.Index index) throws IOException {
        ensureReplicationWrite("index");
        return super.index(index);
    }

    @Override
    public Engine.Index prepareIndex(
        DocumentMapperForType docMapper,
        SourceToParse source,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long autoGeneratedIdTimestamp,
        boolean isRetry,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        ensureReplicationWrite("index");
        return super.prepareIndex(
            docMapper,
            source,
            seqNo,
            primaryTerm,
            version,
            versionType,
            origin,
            autoGeneratedIdTimestamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    @Override
    public Engine.DeleteResult delete(Engine.Delete delete) throws IOException {
        throw unsupported("delete");
    }

    @Override
    public Engine.Delete prepareDelete(
        String id,
        long seqNo,
        long primaryTerm,
        long version,
        VersionType versionType,
        Engine.Operation.Origin origin,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        throw unsupported("delete");
    }

    @Override
    public Engine.NoOpResult noOp(Engine.NoOp noOp) throws IOException {
        // No-op operations are engine/recovery bookkeeping, not a user data
        // write surface. Allow them so sequence-number gaps and primary-term
        // transitions cannot fail a derived shard. Exclude refresh/commit so
        // MAX_SEQ_NO and the checkpoint-only translog metadata describe the
        // same atomic catalog boundary.
        return runUnderRefreshExclusion(() -> super.noOp(noOp));
    }

    private void ensureReplicationWrite(String operation) {
        if (replicationWrite.get() == false) {
            throw unsupported(operation);
        }
    }

    private UnsupportedOperationException unsupported(String operation) {
        return new UnsupportedOperationException(
            "derived index [" + config().getShardId().getIndexName() + "] rejects user " + operation + "; use derived-state replication"
        );
    }

    /**
     * A callable that may throw {@link IOException}.
     * @param <T> the result type
     */
    @FunctionalInterface
    @ExperimentalApi
    public interface IOCallable<T> {
        T call() throws IOException;
    }
}
