/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * MV-specific implementation of {@link DerivedSourceReader}. Owns the
 * {@link MVRemoteSource} that reads the source index's remote segment store
 * for {@code .si} and {@code .parquet} files.
 *
 * <p>This class bridges the format-agnostic {@code fetchSnapshot → downloadToStage}
 * contract to the MV-specific remote-store directory access. The generic
 * {@link org.opensearch.index.engine.derived.pull.DerivedShardPoller} calls
 * these methods without any MV/DataFusion/Parquet awareness.</p>
 */
final class MVDerivedSourceReader implements DerivedSourceReader {

    private static final Logger logger = LogManager.getLogger(MVDerivedSourceReader.class);

    private final IndexSettings indexSettings;
    private final MVPullSettings.Services services;
    private volatile MVRemoteSource source;
    private volatile org.apache.lucene.store.NIOFSDirectory workingDirectory;
    private volatile Path workingPath;

    MVDerivedSourceReader(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.services = services;
    }

    @Override
    public DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) throws IOException {
        ensureInitialized(shard);

        MVRemoteSource.Advert advert = source.latestAdvert(workingDirectory);
        if (advert == null) {
            return null;
        }
        if (advert.maxSeqNo() <= sinceWatermark) {
            return null;
        }

        List<Path> parquetFiles = source.downloadedParquetFiles(workingPath);
        if (parquetFiles.isEmpty()) {
            logger.warn("mv_pull gen {} claims through {} without parquet; no snapshot returned", advert.infosVersion(), advert.maxSeqNo());
            return null;
        }

        return new MVSourceSnapshot(
            shard.shardId().toString(),
            advert.maxSeqNo(),
            advert.primaryTerm(),
            advert.infosVersion(),
            parquetFiles
        );
    }

    @Override
    public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException {
        // Files are already downloaded by fetchSnapshot into the working directory.
        // Copy the parquet files to the stage directory for the builder.
        // Use toString() on getFileName() to avoid ProviderMismatchException when
        // stageDir is a FilterPath (Lucene test framework) and parquetFile is a UnixPath.
        MVSourceSnapshot mvSnapshot = (MVSourceSnapshot) snapshot;
        for (Path parquetFile : mvSnapshot.parquetFiles()) {
            Path target = stageDir.resolve(parquetFile.getFileName().toString());
            Files.copy(parquetFile, target);
        }
    }

    @Override
    public void close() throws IOException {
        if (workingDirectory != null) {
            workingDirectory.close();
        }
    }

    private void ensureInitialized(ShardRouting shard) throws IOException {
        if (source != null) {
            return;
        }
        Settings settings = indexSettings.getSettings();
        DerivedIndexBinding binding = DerivedIndexBinding.fromSettings(settings);
        if (binding == null) {
            throw new IllegalStateException("mv_pull: target shard [" + shard.shardId() + "] has no DerivedIndexBinding");
        }
        services.resolveAndValidateSource(binding);
        String sourceIndexName = binding.sourceName();
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        binding.validateTargetTopology(indexSettings.getNumberOfShards());

        this.source = new MVRemoteSource(services, sourceIndexName, sourceShardId, binding);

        // Working directory for remote file downloads — use tmpdir parent to satisfy forbiddenApis
        Path tmpDir = Path.of(System.getProperty("java.io.tmpdir"));
        this.workingPath = Files.createTempDirectory(tmpDir, "mv_pull_reader");
        this.workingDirectory = new org.apache.lucene.store.NIOFSDirectory(workingPath);
    }

    /**
     * Opaque snapshot carrying MV-specific advert metadata alongside the
     * parquet file list. The generic poller treats this as an opaque token.
     */
    static final class MVSourceSnapshot implements DerivedSourceSnapshot {
        private final String shardId;
        private final long maxSeqNo;
        private final long primaryTerm;
        private final long infosVersion;
        private final List<Path> parquetFiles;

        MVSourceSnapshot(String shardId, long maxSeqNo, long primaryTerm, long infosVersion, List<Path> parquetFiles) {
            this.shardId = shardId;
            this.maxSeqNo = maxSeqNo;
            this.primaryTerm = primaryTerm;
            this.infosVersion = infosVersion;
            this.parquetFiles = List.copyOf(parquetFiles);
        }

        @Override
        public String shardId() {
            return shardId;
        }

        @Override
        public long watermark() {
            return maxSeqNo;
        }

        @Override
        public Map<String, String> metadata() {
            return Map.of("primaryTerm", Long.toString(primaryTerm), "infosVersion", Long.toString(infosVersion));
        }

        long primaryTerm() {
            return primaryTerm;
        }

        long infosVersion() {
            return infosVersion;
        }

        List<Path> parquetFiles() {
            return parquetFiles;
        }
    }
}
