/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Metadata fields managed by the storage layer that must not appear in
 * user-visible {@code _source}. Backends use this set to filter internal
 * columns from document lookup results.
 *
 * @opensearch.internal
 */
public final class ReservedFields {

    private ReservedFields() {}

    /** Fields reserved by the engine/storage layer — excluded from _source reconstruction. */
    public static final Set<String> RESERVED = Set.of(
        "_id",
        "_seq_no",
        "_primary_term",
        "_version",
        "_doc_count",
        "_size",
        "_routing",
        "_ignored",
        "__row_id__"
    );
}
