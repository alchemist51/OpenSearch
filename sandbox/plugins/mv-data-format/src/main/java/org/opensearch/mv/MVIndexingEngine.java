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
public final class MVIndexingEngine
    implements
        IndexingExecutionEngine<org.opensearch.index.engine.dataformat.DerivedDataFormat, MVDocumentInput> {

    /** Definition this engine's writers maintain (SOURCE raw defn or TARGET fold). */
    private final MVDefinitionSpec spec;

    /** The derived format this engine serves (materialized_view on sources, mv_state on targets). */
    private final org.opensearch.index.engine.dataformat.DerivedDataFormat format;

    private final ShardPath shardPath;
    private final String tableName;
    private final String sourceIndexName;
    /** Target MV indices for the separate-index ship path; empty = embedded mode. */
    private final java.util.List<String> shipTargets;
    private final java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier;
    private final java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier;

    public MVIndexingEngine(ShardPath shardPath, String indexName) {
        this(shardPath, indexName, MVDefinitionSpec.SOURCE, MVDataFormat.INSTANCE, java.util.List.of(), () -> null, () -> null);
    }

    public MVIndexingEngine(
        ShardPath shardPath,
        String indexName,
        MVDefinitionSpec spec,
        org.opensearch.index.engine.dataformat.DerivedDataFormat format,
        java.util.List<String> shipTargets,
        java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier,
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier
    ) {
        this.spec = spec;
        this.format = format;
        this.shardPath = shardPath;
        this.sourceIndexName = indexName;
        this.tableName = indexName.replace('-', '_').replace('.', '_');
        this.shipTargets = shipTargets == null ? java.util.List.of() : java.util.List.copyOf(shipTargets);
        this.clientSupplier = clientSupplier;
        this.clusterServiceSupplier = clusterServiceSupplier;
        try {
            Files.createDirectories(shardPath.getDataPath().resolve(getDataFormat().name()));
        } catch (IOException e) {
            throw new RuntimeException("failed to create mv dir", e);
        }
    }

    @Override
    public Writer<MVDocumentInput> createWriter(WriterConfig config) {
        MVStateShipper shipper = null;
        if (shipTargets.isEmpty() == false) {
            org.opensearch.transport.client.Client client = clientSupplier.get();
            if (client == null) {
                throw new IllegalStateException("mv ship targets " + shipTargets + " configured but node client not initialized");
            }
            shipper = new MVStateShipper(client, shipTargets, sourceIndexName, shardPath.getShardId(), clusterServiceSupplier.get(), spec);
        }
        return new MVWriter(config.writerGeneration(), shardPath, tableName, spec, getDataFormat(), shipper);
    }

    @Override
    public Merger getMerger() {
        // Recompute-on-merge (the safe default from the separate-index design,
        // implementation-state §8): derive the merged segment's state by
        // running the definition over the MERGED PRIMARY parquet file. Always
        // consistent with the post-merge document set (and with the future
        // orphan sweep, which is doc-level). The state⊕state fold merger is
        // the later optimization, gated on the sweep's watermark.
        //
        // Ship mode (source with ship targets): merges are a NON-EVENT — no
        // logical data change, nothing to re-ship, no local files to produce.
        return mergeInput -> {
            if (shipTargets.isEmpty() == false) {
                return new org.opensearch.index.engine.dataformat.MergeResult(java.util.Map.of(), null);
            }
            long gen = mergeInput.newWriterGeneration();
            // Merged primary parquet path by the engine's naming convention
            // (POC path coupling, same as the original derived-build).
            java.nio.file.Path parquetDir = shardPath.getDataPath().resolve("parquet");
            // Engine naming: merge outputs are "_parquet_file_generation_merged_<hexgen>"
            // (plain "_parquet_file_generation_<hexgen>" for flush outputs).
            java.nio.file.Path merged = parquetDir.resolve("_parquet_file_generation_merged_" + Long.toHexString(gen) + ".parquet");
            if (java.nio.file.Files.exists(merged) == false) {
                merged = parquetDir.resolve("_parquet_file_generation_" + Long.toHexString(gen) + ".parquet");
            }
            if (java.nio.file.Files.exists(merged) == false) {
                throw new java.io.IOException("mv merge: merged parquet not found for gen " + gen + " in " + parquetDir);
            }
            java.nio.file.Path mvDir = shardPath.getDataPath().resolve(format.name());
            java.nio.file.Files.createDirectories(mvDir);
            java.nio.file.Path out = mvDir.resolve(MVConstants.mvFileName(gen));
            long rows = MVNativeBridge.buildStateFile(merged.toString(), "mv_input", spec.sql(), out.toString());
            org.opensearch.index.engine.exec.MonoFileWriterSet fileSet = org.opensearch.index.engine.exec.MonoFileWriterSet.of(
                mvDir.toAbsolutePath(),
                gen,
                out.getFileName().toString(),
                Math.max(rows, 1)
            );
            return new org.opensearch.index.engine.dataformat.MergeResult(java.util.Map.of(getDataFormat(), fileSet), null);
        };
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
    public org.opensearch.index.engine.dataformat.DerivedDataFormat getDataFormat() {
        return format;
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
        return new MVDocumentInput(spec);
    }

    @Override
    public org.opensearch.index.engine.exec.commit.IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() {}
}
