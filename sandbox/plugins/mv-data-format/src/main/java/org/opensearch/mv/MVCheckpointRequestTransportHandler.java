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
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Source-side handler for {@link MVCheckpointRequestAction}. Reads the local
 * source shard's catalog snapshot and returns a full {@link MVReplicationCheckpoint}
 * to the requesting target shard. This eliminates the target's need for
 * remote-store listing on cold start.
 *
 * <p>Logs: COLD_START_REPLY with file count and maxSeqNo.
 */
public final class MVCheckpointRequestTransportHandler extends HandledTransportAction<
    MVCheckpointRequestAction.Request,
    MVCheckpointRequestAction.Response> {

    private static final Logger logger = LogManager.getLogger(MVCheckpointRequestTransportHandler.class);

    private final IndicesService indicesService;

    @Inject
    public MVCheckpointRequestTransportHandler(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService
    ) {
        super(
            MVCheckpointRequestAction.NAME,
            transportService,
            actionFilters,
            MVCheckpointRequestAction.Request::new,
            ThreadPool.Names.GENERIC
        );
        this.indicesService = indicesService;
    }

    @Override
    protected void doExecute(
        Task task,
        MVCheckpointRequestAction.Request request,
        ActionListener<MVCheckpointRequestAction.Response> listener
    ) {
        try {
            IndexShard shard = null;
            for (IndexService indexService : indicesService) {
                if (indexService.index().getName().equals(request.sourceIndex())) {
                    shard = indexService.getShardOrNull(request.sourceShard());
                    break;
                }
            }
            if (shard == null || !shard.routingEntry().primary() || !shard.routingEntry().active()) {
                logger.debug(
                    "COLD_START_REPLY: source shard [{}][{}] not available",
                    request.sourceIndex(),
                    request.sourceShard()
                );
                listener.onResponse(MVCheckpointRequestAction.Response.unavailable());
                return;
            }

            Map<String, MVFileMetadata> fileMetadata = new LinkedHashMap<>();
            long infosVersion;
            long maxSeqNo;
            long primaryTerm;

            try (GatedCloseable<CatalogSnapshot> ref = shard.getCatalogSnapshot()) {
                CatalogSnapshot catalog = ref.get();
                infosVersion = catalog.getVersion();
                maxSeqNo = shard.getProcessedLocalCheckpoint();
                primaryTerm = shard.getOperationPrimaryTerm();

                for (Segment seg : catalog.getSegments()) {
                    for (Map.Entry<String, WriterFileSet> fsEntry : seg.dfGroupedSearchableFiles().entrySet()) {
                        if (!"parquet".equals(fsEntry.getKey())) continue;
                        WriterFileSet wfs = fsEntry.getValue();
                        Path dir = Path.of(wfs.directory());
                        for (String fileName : wfs.files()) {
                            long size = -1L;
                            try {
                                Path filePath = dir.resolve(fileName);
                                if (Files.exists(filePath)) {
                                    size = Files.size(filePath);
                                }
                            } catch (Exception ignored) {}
                            fileMetadata.put(fileName, new MVFileMetadata(
                                size, wfs.minSeqNo(), wfs.maxSeqNo(), MVFileMetadata.CRC32_UNKNOWN
                            ));
                        }
                    }
                }
            }

            MVReplicationCheckpoint checkpoint = new MVReplicationCheckpoint(
                request.sourceIndex(),
                request.sourceShard(),
                primaryTerm,
                maxSeqNo,
                infosVersion,
                fileMetadata,
                System.currentTimeMillis()
            );

            logger.info(
                "COLD_START_REPLY source=[{}][{}] target=[{}][{}] files={} maxSeqNo={}",
                request.sourceIndex(),
                request.sourceShard(),
                request.targetIndex(),
                request.targetShard(),
                fileMetadata.size(),
                maxSeqNo
            );

            listener.onResponse(new MVCheckpointRequestAction.Response(checkpoint));
        } catch (Exception e) {
            logger.warn(
                "COLD_START_REPLY: failed for source=[{}][{}] target=[{}][{}]",
                request.sourceIndex(),
                request.sourceShard(),
                request.targetIndex(),
                request.targetShard(),
                e
            );
            listener.onFailure(e);
        }
    }
}
