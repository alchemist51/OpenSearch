/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.opensearch.common.settings.Setting;

public class ParquetSettings {

    public static final Setting<Integer> INDEX_PARQUET_BATCH_SIZE = Setting.intSetting(
        "index.parquet.batch_size",
        1024,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final Setting<Integer> CLUSTER_PARQUET_BATCH_SIZE = Setting.intSetting(
        "cluster.parquet.batch_size",
        1024,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );


}
