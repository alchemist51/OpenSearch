/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Tests for {@link LuceneSearchEnginePlugin}.
 */
public class LuceneSearchEnginePluginTests extends OpenSearchTestCase {

    /**
     * Test that getCommitter() returns a non-empty Optional containing
     * a LuceneCommitter instance.
     *
     * Validates: Requirements 4.2
     */
    public void testGetCommitterReturnsLuceneCommitter() throws IOException {
        Path baseDir = createTempDir();
        ShardId shardId = new ShardId("test", "_na_", 0);
        Path dataPath = baseDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(dataPath);
        ShardPath shardPath = new ShardPath(false, dataPath, dataPath, shardId);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        CommitterSettings committerSettings = new CommitterSettings(shardPath, indexSettings);

        LuceneSearchEnginePlugin plugin = new LuceneSearchEnginePlugin();
        Optional<Committer> committer = plugin.getCommitter(committerSettings);

        assertTrue("getCommitter() should return a non-empty Optional", committer.isPresent());
        assertTrue("getCommitter() should return a LuceneCommitter instance", committer.get() instanceof LuceneCommitter);
        committer.get().close();
    }
}
