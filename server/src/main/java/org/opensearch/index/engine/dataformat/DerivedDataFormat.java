/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import java.util.Set;

/**
 * Base type for DERIVED data formats: formats whose per-segment output is
 * computed FROM the ingested documents rather than being a storage of them
 * (e.g. materialized-view aggregate state). The type codifies the contract
 * that individual overrides previously expressed piecemeal:
 *
 * <ul>
 *   <li><b>Row parity exempt</b> — derived output is per-group, not per-doc;
 *       its row counts legally differ from storage formats'.</li>
 *   <li><b>May emit no files</b> — a derived format may skip a generation
 *       (skip-and-backfill) or ship its output elsewhere entirely (the
 *       separate-index MV target); an empty flush result is legal and the
 *       composite engine treats the format as optional per segment.</li>
 *   <li><b>Never claims fields</b> — derived formats observe the composite
 *       broadcast; they do not participate in capability assignment and never
 *       win a claim ({@code priority()} = MAX, {@code supportedFields()}
 *       empty).</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public abstract class DerivedDataFormat extends DataFormat {

    @Override
    public final long priority() {
        return Long.MAX_VALUE;
    }

    @Override
    public final Set<FieldTypeCapabilities> supportedFields() {
        return Set.of();
    }

    @Override
    public final boolean exemptFromRowParity() {
        return true;
    }

    @Override
    public final boolean mayEmitNoFiles() {
        return true;
    }
}
