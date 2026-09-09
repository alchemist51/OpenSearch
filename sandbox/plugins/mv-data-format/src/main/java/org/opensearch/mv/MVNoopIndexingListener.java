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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexingOperationListener;

/**
 * Plugin-safe {@link IndexingOperationListener} that records seqNos which
 * consume a sequence number but do NOT produce a parquet row. Registered on
 * SOURCE indices via {@code IndexModule.addIndexOperationListener} in
 * {@link MVDataFormatPlugin#onIndexModule}. Never modifies or touches the
 * engine — pure observation.
 *
 * <h2>SeqNo-consuming-no-row cases</h2>
 * <ol>
 *   <li><b>postIndex with FAILURE + assigned seqNo</b>: a mapping parse failure
 *       on the primary. The engine assigns the seqNo (it's in the translog) but
 *       no Lucene document (and thus no parquet row) is produced. Detected by
 *       {@code result.getResultType() == FAILURE && result.getSeqNo() >= 0}.</li>
 *   <li><b>postDelete with assigned seqNo</b>: ALL deletes (success or failure)
 *       consume a seqNo without producing a parquet row. Detected by
 *       {@code result.getSeqNo() >= 0}. (Note: successful deletes have
 *       {@code resultType == SUCCESS} but still produce no parquet row.)</li>
 * </ol>
 *
 * <h2>Cases that do NOT consume a seqNo</h2>
 * <ul>
 *   <li>{@code MAPPING_UPDATE_REQUIRED}: coordinating-node retry, no seqNo assigned
 *       ({@code UNASSIGNED_SEQ_NO})</li>
 *   <li>{@code IndexResult(Exception, long version)} (2-arg failure): pre-engine
 *       failure, {@code UNASSIGNED_SEQ_NO}</li>
 *   <li>{@code Engine.NoOp}: seqNo-filling on replicas (gap-fill noop) — goes
 *       through {@code IndexShard.markSeqNoAsNoop} which does NOT invoke
 *       {@link IndexingOperationListener}. These seqNos also don't produce
 *       parquet rows but are invisible to us. Fortunately, replica noop gaps
 *       don't occur on primaries where MV source tracking happens.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public final class MVNoopIndexingListener implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(MVNoopIndexingListener.class);
    private static final long UNASSIGNED_SEQ_NO = -2L; // Engine.UNASSIGNED_SEQ_NO is package-private; -2 is the constant

    private final MVNoopTracker tracker;

    public MVNoopIndexingListener(MVNoopTracker tracker) {
        this.tracker = tracker;
    }

    /**
     * Records a noop for index operations that FAIL after seqNo assignment.
     * The engine writes a translog entry (consuming the seqNo) but produces no
     * Lucene document and thus no parquet row.
     */
    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getResultType() == Engine.Result.Type.FAILURE && result.getSeqNo() >= 0) {
            tracker.recordNoop(shardId, result.getSeqNo());
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "mv_noop_listener: recorded FAILURE index noop seqNo={} shard=[{}]",
                    result.getSeqNo(),
                    shardId
                );
            }
        }
    }

    /**
     * ALL deletes consume a seqNo without producing a parquet row, regardless
     * of whether the delete found the document or not.
     */
    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getSeqNo() >= 0) {
            tracker.recordNoop(shardId, result.getSeqNo());
            if (logger.isDebugEnabled()) {
                logger.debug(
                    "mv_noop_listener: recorded delete noop seqNo={} shard=[{}] found={}",
                    result.getSeqNo(),
                    shardId,
                    result.isFound()
                );
            }
        }
    }
}
