/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.lucene;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.FieldSupportRegistry;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.lucene.engine.LuceneExecutionEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FormatStoreDirectory;
import org.opensearch.plugins.DataSourcePlugin;

import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Set;

public class LuceneDataSourcePlugin extends Plugin implements DataSourcePlugin {

    @Override
    @SuppressWarnings("unchecked")
    public <T extends DataFormat> IndexingExecutionEngine<T> indexingEngine(EngineConfig engineConfig, MapperService mapperService, boolean isPrimary, ShardPath shardPath, IndexSettings indexSettings, FieldAssignments fieldAssignments) {
        return (IndexingExecutionEngine<T>) new LuceneExecutionEngine(engineConfig, mapperService, isPrimary, shardPath, indexSettings, fieldAssignments);
    }

    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        return null;
    }

    @Override
    public BlobContainer createBlobContainer(BlobStore blobStore, BlobPath blobPath) throws IOException {
        return null;
    }

    @Override
    public DataFormat getDataFormat() {
        return new LuceneDataFormat();
    }

    @Override
    public void registerFieldSupport(FieldSupportRegistry registry) {
        DataFormat lucene = getDataFormat();
        Set<FieldCapability> allCaps = EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX, FieldCapability.DOC_VALUES);
        Set<FieldCapability> storeAndIndex = EnumSet.of(FieldCapability.STORE, FieldCapability.INDEX);

        // Most field types support all three capabilities
        String[] fullSupportTypes = {
            "keyword", "long", "integer", "short", "byte", "double", "float", "half_float", "scaled_float",
            "date", "date_nanos", "boolean", "ip", "geo_point", "geo_shape", "binary", "integer_range",
            "float_range", "long_range", "double_range", "date_range", "ip_range", "completion",
            "search_as_you_type", "token_count", "murmur3", "flat_object", "unsigned_long"
        };
        for (String type : fullSupportTypes) {
            registry.register(type, lucene, allCaps);
        }

        // text fields support STORE and INDEX but not DOC_VALUES
        registry.register("text", lucene, storeAndIndex);
        registry.register("match_only_text", lucene, storeAndIndex);
        registry.register("annotated_text", lucene, storeAndIndex);
    }

}
