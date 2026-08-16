/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POC(mv) per-shard indexing engine for the derived materialized-view format.
 * Pass-through refresh (parquet-engine pattern); merges deliberately
 * unsupported in the POC.
 */
public final class MVIndexingEngine implements IndexingExecutionEngine<MVDataFormat, MVDocumentInput> {

    private final ShardPath shardPath;
    private final String tableName;
    private final String sourceIndexName;
    /** Target MV index for the separate-index ship path; null = embedded mode. */
    private final String shipTarget;
    private final java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier;
    private final java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier;

    public MVIndexingEngine(ShardPath shardPath, String indexName) {
        this(shardPath, indexName, null, () -> null, () -> null);
    }

    public MVIndexingEngine(
        ShardPath shardPath,
        String indexName,
        String shipTarget,
        java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier,
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier
    ) {
        this.shardPath = shardPath;
        this.sourceIndexName = indexName;
        this.tableName = indexName.replace('-', '_').replace('.', '_');
        this.shipTarget = shipTarget;
        this.clientSupplier = clientSupplier;
        this.clusterServiceSupplier = clusterServiceSupplier;
        try {
            Files.createDirectories(shardPath.getDataPath().resolve(MVConstants.DIR));
        } catch (IOException e) {
            throw new RuntimeException("failed to create mv dir", e);
        }
    }

    @Override
    public Writer<MVDocumentInput> createWriter(WriterConfig config) {
        MVStateShipper shipper = null;
        if (shipTarget != null) {
            org.opensearch.transport.client.Client client = clientSupplier.get();
            if (client == null) {
                throw new IllegalStateException("mv ship target [" + shipTarget + "] configured but node client not initialized");
            }
            shipper = new MVStateShipper(client, shipTarget, sourceIndexName, shardPath.getShardId(), clusterServiceSupplier.get());
        }
        return new MVWriter(config.writerGeneration(), shardPath, tableName, shipper);
    }

    @Override
    public Merger getMerger() {
        // POC: merges disabled for the demo index (merge_on_refresh off, no
        // background force-merge in the test). Fails loudly if one sneaks in.
        return mergeInput -> { throw new UnsupportedOperationException("POC(mv): merge not implemented"); };
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) {
        if (refreshInput == null) {
            return new RefreshResult(List.of());
        }
        List<Segment> segments = new ArrayList<>();
        segments.addAll(refreshInput.existingSegments());
        segments.addAll(refreshInput.writerFiles());
        return new RefreshResult(List.copyOf(segments));
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("generation is owned by DataFormatAwareEngine");
    }

    @Override
    public MVDataFormat getDataFormat() {
        return MVDataFormat.INSTANCE;
    }

    @Override
    public long getHeapBytesUsed() {
        return 0;
    }

    @Override
    public long getNativeBytesUsed() {
        return 0;
    }

    @Override
    public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) {
        Map<String, Collection<String>> failed = new HashMap<>();
        Collection<String> mvFiles = filesToDelete.get(MVDataFormat.NAME);
        if (mvFiles == null) {
            return failed;
        }
        Path dir = shardPath.getDataPath().resolve(MVConstants.DIR);
        List<String> failures = new ArrayList<>();
        for (String f : mvFiles) {
            try {
                Files.deleteIfExists(dir.resolve(f));
            } catch (IOException e) {
                failures.add(f);
            }
        }
        if (failures.isEmpty() == false) {
            failed.put(MVDataFormat.NAME, failures);
        }
        return failed;
    }

    @Override
    public MVDocumentInput newDocumentInput() {
        return new MVDocumentInput();
    }

    @Override
    public org.opensearch.index.engine.exec.commit.IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() {}
}
