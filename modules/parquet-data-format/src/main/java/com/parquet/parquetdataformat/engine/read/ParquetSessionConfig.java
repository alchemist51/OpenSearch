/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.vectorized.execution.search.spi.ConfigUpdateListener;
import org.opensearch.vectorized.execution.search.spi.SessionConfig;
import org.opensearch.vectorized.execution.search.spi.SessionConfigRegistry;

public class ParquetSessionConfig implements SessionConfig {

    SessionConfigRegistry sessionConfigRegistry;
    Integer parquetBatchSize;

    ParquetSessionConfig() {
        super();
        //sessionConfigRegistry = new SessionConfigRegistry();

    }

    @Override
    public void setBatchSize(int batchSize){
        parquetBatchSize = batchSize;
//        updateParquetSessionConfig(nativeSessionConfigPtr, "batch_size", String.valueOf(batchSize));
        sessionConfigRegistry.publishSessionConfigUpdate(this);
    }

    @Override
    public void mergeFrom(SessionConfig other) {
        throw new UnsupportedOperationException("Parquet can't merge from other config");
    }

    @Override
    public Integer getBatchSize() {
        return indexSettings.getAsBoolean(
            IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(),
            clusterSettings.getOrNull(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
    }

}
