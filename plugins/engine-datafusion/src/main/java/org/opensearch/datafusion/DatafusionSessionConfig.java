/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.vectorized.execution.search.spi.SessionConfig;

public class DatafusionSessionConfig implements SessionConfig {

    private final long ptr;

    public DatafusionSessionConfig() {
        this.ptr = createDefaultNativeSessionConfigPtr();
    }


    @Override
    public Integer getBatchSize() {
        return getParquetSessionConfigValue(ptr, "batch_size");
    }

    @Override
    public void setBatchSize(int batchSize) {
        updateParquetSessionConfig(ptr, "batch_size", batchSize);
    }

    @Override
    public void mergeFrom(SessionConfig other) {
        // If not null, means it needs to be overridden
        if(other.getBatchSize() != null) {
            setBatchSize(other.getBatchSize());
        }
    }

    native static long createDefaultNativeSessionConfigPtr();
    native static void updateParquetSessionConfig(long ptr, String key, int value);
    native static int getParquetSessionConfigValue(long ptr, String key);
}
