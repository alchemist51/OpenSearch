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
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POC(mv) separate-index ship path: exports a finalized state file's rows and
 * synchronously bulk-indexes them into the target MV index.
 *
 * <p><b>Ship-before-commit</b>: the caller ({@code MVWriter.flush}) invokes
 * {@link #ship} BEFORE returning its flush result; any failure here throws,
 * the flush fails, and the source does not commit — the data-level invariant
 * (committed-on-source ⇒ present-on-target) holds by construction.
 *
 * <p><b>Idempotence</b>: doc IDs are deterministic
 * ({@code <sourceIndex>.<shard>.<generation>.<row>}), so a retried flush
 * (after a failed ship) overwrites rather than duplicates — re-shipping the
 * same generation is safe.
 *
 * <p><b>Visibility</b>: refresh policy NONE — ship+ack guarantees durable
 * presence; rows become searchable after the MV index's own refresh (design
 * decision 6 in the separate-index README).
 *
 * <p>POC simplifications (documented in the separate-index folder): the state
 * rows travel as parsed TSV from the native export rather than Arrow batches
 * over a dedicated transport action; the synchronous bulk blocks the flushing
 * thread (production wants a ship executor + ack listener); no colocation
 * fast path (that is the resize-precedent Phase 1/2 work).
 */
final class MVStateShipper {

    private static final Logger logger = LogManager.getLogger(MVStateShipper.class);

    private final Client client;
    private final String targetIndex;
    private final String sourceIndex;
    private final ShardId shardId;
    private final org.opensearch.cluster.service.ClusterService clusterService;

    MVStateShipper(
        Client client,
        String targetIndex,
        String sourceIndex,
        ShardId shardId,
        org.opensearch.cluster.service.ClusterService clusterService
    ) {
        this.client = client;
        this.targetIndex = targetIndex;
        this.sourceIndex = sourceIndex;
        this.shardId = shardId;
        this.clusterService = clusterService;
    }

    /**
     * Ships every state row of {@code stateFile} to the target MV index and
     * blocks until the bulk is acked. Throws on ANY failure — the caller must
     * fail the flush (ship-before-commit).
     *
     * @return number of state rows shipped
     */
    long ship(String stateFile, long writerGeneration) throws IOException {
        String tsv = MVNativeBridge.searchV2(List.of(stateFile), MVConstants.EXPORT_SQL);
        if (tsv.isEmpty()) {
            return 0;
        }
        String[] lines = tsv.split("\n");
        List<String> docIds = new ArrayList<>();
        List<Map<String, Object>> docs = new ArrayList<>();
        int row = 0;
        for (String line : lines) {
            if (line.isEmpty()) continue;
            String[] cols = line.split("\t");
            if (cols.length != MVConstants.SHIP_FIELDS.size()) {
                throw new IOException(
                    "mv ship: state row has " + cols.length + " columns, expected " + MVConstants.SHIP_FIELDS.size() + ": " + line
                );
            }
            Map<String, Object> doc = new HashMap<>();
            doc.put(MVConstants.SHIP_FIELDS.get(0), cols[0]);
            doc.put(MVConstants.SHIP_FIELDS.get(1), cols[1]);
            for (int i = 2; i < cols.length; i++) {
                doc.put(MVConstants.SHIP_FIELDS.get(i), Long.parseLong(cols[i]));
            }
            // Provenance fields: which source shard/generation produced this state row.
            doc.put("_mv_source_index", sourceIndex);
            doc.put("_mv_source_shard", shardId.id());
            doc.put("_mv_source_generation", writerGeneration);
            docIds.add(sourceIndex + "." + shardId.id() + "." + writerGeneration + "." + row);
            docs.add(doc);
            row++;
        }
        // Shard-addressed ship to the ORDINAL-PAIRED target shard (source shard
        // i -> target shard i; a doc-routed bulk would spray across all target
        // shards and defeat the colocation pairing). The transport handler
        // applies locally when colocated (no serialization) and forwards once
        // when the pair is split; either way the ack means durable-in-translog.
        int targetShard = shardId.id() % clusterService.state().metadata().index(targetIndex).getNumberOfShards();
        MVShipStateAction.Request request = new MVShipStateAction.Request(targetIndex, targetShard, docIds, docs);
        MVShipStateAction.Response response;
        try {
            response = client.execute(MVShipStateAction.INSTANCE, request).actionGet();
        } catch (Exception e) {
            throw new IOException(
                "mv ship: transport ship to [" + targetIndex + "][" + targetShard + "] failed gen=" + writerGeneration,
                e
            );
        }
        logger.info(
            "mv ship: gen={} shipped {} state rows from {}[{}] -> [{}][{}] (acked durable)",
            writerGeneration,
            response.applied(),
            sourceIndex,
            shardId.id(),
            targetIndex,
            targetShard
        );
        return response.applied();
    }
}
