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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Builder-shard emulation, follower side: a {@link DerivedSourceReader} whose
 * "source" is the leader's {@link MVBuilderOutbox} for this target, not the
 * source index. One {@code latest.json} GET per poll; the {@code prev} chain is
 * walked only when the follower is behind by more than one publication.
 */
final class MVBuilderOutboxReader implements DerivedSourceReader {

    private static final Logger logger = LogManager.getLogger(MVBuilderOutboxReader.class);

    private final IndexSettings indexSettings;
    private final MVPullSettings.Services services;
    private final DerivedIndexBinding binding;
    private volatile MVBuilderOutbox outbox;

    MVBuilderOutboxReader(IndexSettings indexSettings, MVPullSettings.Services services) {
        this.indexSettings = indexSettings;
        this.services = services;
        this.binding = DerivedIndexBinding.fromSettings(indexSettings.getSettings());
        if (binding == null || binding.sourceName() == null) {
            throw new IllegalStateException("mv_pull hydrate: target [" + indexSettings.getIndex().getName() + "] has no source binding");
        }
    }

    @Override
    public DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) throws IOException {
        int sourceShardId = binding.resolveSourceShard(shard.shardId().id());
        // The poller starts at -1 after a restart; the hydrate builder republishes the
        // recovered watermark here so we do not re-walk (and re-download) applied history.
        long since = Math.max(sinceWatermark, MVHydrateArtifactBuilder.appliedWatermark(shard.shardId()));
        List<MVBuilderOutbox.Publication> pubs = outbox(sourceShardId).since(since);
        if (pubs.isEmpty()) {
            return null;
        }
        MVBuilderOutbox.Publication last = pubs.get(pubs.size() - 1);
        return new HydrateSnapshot(shard.shardId().toString(), last.toInclusive(), pubs);
    }

    @Override
    public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException {
        HydrateSnapshot hs = (HydrateSnapshot) snapshot;
        MVBuilderOutbox box = outbox;
        long t0 = System.nanoTime();
        long bytes = 0;
        for (MVBuilderOutbox.Publication pub : hs.publications()) {
            box.download(pub, stageDir.resolve(pub.stateBlob()));
            bytes += pub.stateBytes();
        }
        logger.debug(
            "mv_pull HYDRATE_DOWNLOAD target=[{}] publications={} bytes={} ms={}",
            indexSettings.getIndex().getName(),
            hs.publications().size(),
            bytes,
            (System.nanoTime() - t0) / 1_000_000
        );
    }

    @Override
    public void close() {}

    private MVBuilderOutbox outbox(int sourceShardId) {
        MVBuilderOutbox box = outbox;
        if (box == null) {
            IndexMetadata source = services.sourceIndexMetadata(binding.sourceName());
            String repository = source.getSettings().get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
            if (repository == null) {
                throw new IllegalStateException("mv_pull hydrate: source index [" + binding.sourceName() + "] has no remote segment store");
            }
            box = MVBuilderOutbox.open(
                services.repositoriesService(),
                repository,
                source.getIndexUUID(),
                sourceShardId,
                indexSettings.getIndex().getName()
            );
            outbox = box;
        }
        return box;
    }

    /** The publications a follower must apply, oldest first; watermark = the newest {@code to}. */
    record HydrateSnapshot(String shardId, long watermark, List<MVBuilderOutbox.Publication> publications)
        implements
            DerivedSourceSnapshot {
        @Override
        public Map<String, String> metadata() {
            return Map.of("publications", Integer.toString(publications.size()), "mode", MVPullSettings.MODE_HYDRATE);
        }
    }
}
