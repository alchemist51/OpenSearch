/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet.substrait;

public class SchemaDefinition {
    public static String createSchemaDefinition() {
        return "CREATE TABLE LOGS ("
            + "backend_ip VARCHAR, "
            + "backend_port INTEGER, "
            + "backend_processing_time FLOAT, "
            + "backend_status_code INTEGER, "
            + "client_ip VARCHAR, "
            + "client_port INTEGER, "
            + "connection_time FLOAT, "
            + "destination_ip VARCHAR, "
            + "destination_port INTEGER, "
            + "elb_status_code INTEGER, "
            + "http_port INTEGER, "
            + "http_version VARCHAR, "
            + "matched_rule_priority INTEGER, "
            + "received_bytes INTEGER, "
            + "request_creation_time BIGINT, "
            + "request_processing_time FLOAT, "
            + "response_processing_time FLOAT, "
            + "sent_bytes INTEGER, "
            + "target_ip VARCHAR, "
            + "target_port INTEGER, "
            + "target_processing_time FLOAT, "
            + "target_status_code INTEGER, "
            + "timestamp_col BIGINT)";
    }
}
