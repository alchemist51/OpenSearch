/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.engine.read;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.vectorized.execution.search.spi.SessionConfigRegistry;
import org.opensearch.vectorized.execution.search.spi.ConfigUpdateListener;
import org.opensearch.vectorized.execution.search.spi.EngineConfig;

public class ParquetConfig {

    private boolean enablePruning;
    ParquetSessionConfig parquetSessionConfig;
    ParquetNativeConfiguration parquetNativeConfiguration;

    ParquetConfig() {
        parquetSessionConfig = new ParquetSessionConfig();
        parquetNativeConfiguration = new ParquetNativeConfiguration();
    }

    public void registerListener(ConfigUpdateListener listener) {
        parquetSessionConfig.registerListener(listener);
    }

    public EngineConfig updateEngineConfig(EngineConfig engineConfig) {
        return engineConfig
            .updateNativeConfiguration(parquetNativeConfiguration)
            .updateSessionConfig(parquetSessionConfig);
    }
}
