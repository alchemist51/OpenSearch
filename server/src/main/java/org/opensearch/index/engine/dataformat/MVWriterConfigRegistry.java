/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server-level SPI registry that bridges the MV definition layer (mv-engine plugin)
 * to the writer layer (parquet-data-format plugin) without introducing a compile-time
 * dependency between them.
 *
 * <p>The mv-engine plugin registers a {@link MVWriterSpecCompiler} at plugin construction.
 * The parquet-data-format plugin calls {@link #compileSpecs(Map)} during writer-open to
 * obtain FFI-ready specs. If no compiler is registered (MV plugin not loaded), the method
 * returns an empty list — no MV builders are created.</p>
 *
 * <p>This lives in the server module's dataformat SPI package, which both plugins depend on.</p>
 *
 * @opensearch.internal
 */
public final class MVWriterConfigRegistry {

    private MVWriterConfigRegistry() {}

    private static final AtomicReference<MVWriterSpecCompiler> COMPILER = new AtomicReference<>();

    /**
     * Register the MV spec compiler. Called by mv-engine plugin during construction.
     */
    public static void register(MVWriterSpecCompiler compiler) {
        if (COMPILER.compareAndSet(null, compiler) == false) {
            throw new IllegalStateException("MVWriterSpecCompiler already registered");
        }
    }

    /**
     * Unregister the compiler. Called on plugin teardown.
     */
    public static void unregister() {
        COMPILER.set(null);
    }

    /**
     * Compile MV definitions from an index's customData into FFI-ready specs.
     *
     * @param customData the IndexMetadata.customData map
     * @return list of compiled specs (empty if no MV definitions or no compiler)
     */
    public static List<MVPartialWriterSpec> compileSpecs(Map<String, String> customData) {
        MVWriterSpecCompiler compiler = COMPILER.get();
        if (compiler == null) {
            return List.of();
        }
        return compiler.compile(customData);
    }

    /**
     * SPI interface implemented by the mv-engine plugin.
     */
    @FunctionalInterface
    public interface MVWriterSpecCompiler {
        List<MVPartialWriterSpec> compile(Map<String, String> customData);
    }

    /**
     * FFI-ready spec for one MV, consumed by the parquet writer's native bridge.
     * Pure data record with no dependencies on mv-engine internals.
     */
    public record MVPartialWriterSpec(
        String mvId,
        String definitionHash,
        long defVersion,
        List<String> groupColNames,
        List<String> groupColTypes,
        List<AggFFI> aggSpecs,
        List<String> sortKeyNames
    ) {
        /**
         * One aggregate specification.
         */
        public record AggFFI(String function, String sourceField, List<String> outputNames) {}
    }
}
