/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.index.engine.dataformat.MVWriterConfigRegistry;

/**
 * Bridges MV definitions from IndexMetadata customData to the Rust parquet
 * writer's MV partial builder via the FFI writer-config path.
 *
 * <p>Source-side builder plumbing (c2): when a ParquetWriter is created for
 * a source index that carries {@code mv_definitions} in its customData, this
 * class compiles each definition into an {@link MVPartialWriterSpec} that the
 * Rust FFI can consume.</p>
 */
public final class MVWriterConfig {

    private static final Logger logger = LogManager.getLogger(MVWriterConfig.class);

    private MVWriterConfig() {}

    /**
     * A single MV's spec ready for FFI serialization to Rust.
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
        public record AggFFI(String function, String sourceField, List<String> outputNames) {}
    }

    /**
     * Extract MV definitions from the source index's customData and compile
     * them into FFI-ready specs.
     *
     * @param customData the IndexMetadata.customData map (key = MVConstants.SOURCE_DEFINITIONS_KEY)
     * @return list of specs (empty if no MV definitions)
     */
    public static List<MVPartialWriterSpec> fromCustomData(Map<String, String> customData) {
        if (customData == null) {
            return Collections.emptyList();
        }
        String defsJson = customData.get(MVConstants.SOURCE_DEFINITIONS_KEY);
        if (defsJson == null || defsJson.isBlank()) {
            return Collections.emptyList();
        }

        List<MVPartialWriterSpec> specs = new ArrayList<>();
        try {
            // Parse the outer map: { "mvId1": { descriptor }, "mvId2": { descriptor }, ... }
            try (XContentParser parser = XContentType.JSON.xContent().createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, defsJson)) {
                parser.nextToken(); // START_OBJECT
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    String mvId = parser.currentName();
                    parser.nextToken(); // move to the descriptor object
                    try {
                        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.fromXContent(parser);
                        MVCompiledDefinition compiled = MVCompiledDefinition.fromDescriptor(descriptor);
                        MVPartialWriterSpec spec = compileToFFISpec(mvId, compiled);
                        specs.add(spec);
                        logger.info("Compiled MV definition for source-side builder: mvId={}, hash={}",
                            mvId, compiled.hash());
                    } catch (Exception e) {
                        logger.error("Failed to compile MV definition mvId={}: {}", mvId, e.getMessage());
                        // Skip — don't fail writer creation for one bad definition
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Failed to parse mv_definitions from customData: {}", e.getMessage());
        }
        return specs;
    }

    /**
     * Compile a single MVCompiledDefinition into an FFI-ready spec.
     */
    static MVPartialWriterSpec compileToFFISpec(String mvId, MVCompiledDefinition compiled) {
        List<String> groupColNames = new ArrayList<>();
        List<String> groupColTypes = new ArrayList<>();
        List<String> sortKeyNames = new ArrayList<>();

        for (GroupKey key : compiled.groupKeys()) {
            groupColNames.add(key.name());
            groupColTypes.add(arrowTypeString(key.columnType()));
            sortKeyNames.add(key.name());
        }

        List<MVPartialWriterSpec.AggFFI> aggSpecs = new ArrayList<>();
        for (AggregateSpec agg : compiled.aggregates()) {
            List<String> outputNames = new ArrayList<>();
            for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                outputNames.add(sc.name());
            }
            String funcName = agg.function().name().toLowerCase();
            // Distinguish COUNT(*) from COUNT(field) — the Rust side needs this
            if (agg.function() == AggregateSpec.AggFunction.COUNT && agg.sourceField() != null) {
                funcName = "count_field";
            }
            aggSpecs.add(new MVPartialWriterSpec.AggFFI(funcName, agg.sourceField(), outputNames));
        }

        return new MVPartialWriterSpec(
            mvId,
            compiled.hash(),
            1L,
            groupColNames,
            groupColTypes,
            aggSpecs,
            sortKeyNames
        );
    }

    private static String arrowTypeString(GroupKey.ColumnType type) {
        return switch (type) {
            case KEYWORD -> "utf8";
            case LONG -> "int64";
            case INTEGER -> "int32";
            case DOUBLE -> "float64";
            case TIMESTAMP -> "timestamp_ms";
        };
    }

    /**
     * Bridge method: compiles customData into server-level registry specs.
     * Used by MVEnginePlugin to register with MVWriterConfigRegistry.
     */
    public static List<MVWriterConfigRegistry.MVPartialWriterSpec> fromCustomDataToRegistrySpecs(Map<String, String> customData) {
        List<MVPartialWriterSpec> internalSpecs = fromCustomData(customData);
        List<MVWriterConfigRegistry.MVPartialWriterSpec> registrySpecs = new ArrayList<>(internalSpecs.size());
        for (MVPartialWriterSpec s : internalSpecs) {
            List<MVWriterConfigRegistry.MVPartialWriterSpec.AggFFI> aggFFIs = new ArrayList<>(s.aggSpecs().size());
            for (MVPartialWriterSpec.AggFFI a : s.aggSpecs()) {
                aggFFIs.add(new MVWriterConfigRegistry.MVPartialWriterSpec.AggFFI(a.function(), a.sourceField(), a.outputNames()));
            }
            registrySpecs.add(new MVWriterConfigRegistry.MVPartialWriterSpec(
                s.mvId(), s.definitionHash(), s.defVersion(),
                s.groupColNames(), s.groupColTypes(), aggFFIs, s.sortKeyNames()
            ));
        }
        return registrySpecs;
    }
}
