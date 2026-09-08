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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MV query read service: executes aggregation queries against hydrated partial
 * state files via the DataFusion native stack ({@code df_mv_query_state} FFI).
 *
 * <p>Production path would integrate with the SearchBackEndPlugin to intercept
 * aggregation queries on MV targets and route through this stack transparently.
 * POC exposes a direct REST action: {@code POST /_mv/{target}/_query}.</p>
 *
 * @opensearch.internal
 */
public class MVReadService {

    private static final Logger logger = LogManager.getLogger(MVReadService.class);

    private final ClusterService clusterService;

    public MVReadService(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Query the MV target's hydrated partials via the native DataFusion fold.
     * Returns the result rows as a list of maps (group keys + final aggregate values).
     *
     * @param targetIndex the MV target index name
     * @param targetShardDataPath path to the target shard's data directory
     * @return query result: list of rows, each row is a map of column -> value
     */
    public MVQueryResult query(String targetIndex, Path targetShardDataPath) throws IOException {
        // Resolve the MV definition from the target's mv_binding customData.
        IndexMetadata targetMeta = clusterService.state().metadata().index(targetIndex);
        if (targetMeta == null) {
            throw new IllegalArgumentException("Target index [" + targetIndex + "] not found");
        }
        Map<String, String> mvBinding = targetMeta.getCustomData(MVConstants.TARGET_BINDING_KEY);
        if (mvBinding == null || mvBinding.isEmpty()) {
            throw new IllegalArgumentException("Index [" + targetIndex + "] is not an MV target (no mv_binding)");
        }

        // Reconstruct the compiled definition from descriptor in binding.
        String descriptorJson = mvBinding.get("descriptor");
        if (descriptorJson == null) {
            throw new IllegalArgumentException("mv_binding missing descriptor for [" + targetIndex + "]");
        }
        MVDefinitionDescriptor descriptor;
        try (var xParser = org.opensearch.common.xcontent.json.JsonXContent.jsonXContent
                .createParser(org.opensearch.core.xcontent.NamedXContentRegistry.EMPTY,
                    org.opensearch.core.xcontent.DeprecationHandler.IGNORE_DEPRECATIONS,
                    descriptorJson)) {
            descriptor = MVDefinitionDescriptor.fromXContent(xParser);
        }
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);

        // Build the definition SQL for the partial fold.
        String definitionSql = def.buildPartialSql(MVConstants.INPUT_TABLE);

        // Resolve source index to get the input schema.
        String sourceIndex = mvBinding.get("source_index");
        IndexMetadata sourceMeta = sourceIndex != null ? clusterService.state().metadata().index(sourceIndex) : null;
        // Build a minimal Arrow schema JSON from the definition's group keys + aggregates
        // (the source fields the definition references).
        String schemaJson = buildInputSchemaJson(def, sourceMeta);

        // Discover hydrated shard directories.
        Path hydratedDir = targetShardDataPath.resolve("mv_hydrated");
        List<String> shardDirs = discoverShardDirs(hydratedDir);
        if (shardDirs.isEmpty()) {
            return MVQueryResult.empty(def);
        }

        // Call native FFI.
        return executeNativeQuery(shardDirs, definitionSql, schemaJson, def);
    }

    private List<String> discoverShardDirs(Path hydratedDir) throws IOException {
        List<String> dirs = new ArrayList<>();
        if (!Files.isDirectory(hydratedDir)) {
            return dirs;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(hydratedDir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    // Verify it has at least one .parquet file
                    try (DirectoryStream<Path> files = Files.newDirectoryStream(entry, "*.parquet")) {
                        if (files.iterator().hasNext()) {
                            dirs.add(entry.toAbsolutePath().toString());
                        }
                    }
                }
            }
        }
        return dirs;
    }

    @SuppressWarnings("try")
    private MVQueryResult executeNativeQuery(
        List<String> shardDirs,
        String definitionSql,
        String schemaJson,
        MVCompiledDefinition def
    ) throws IOException {
        // Serialize dirs as JSON array.
        StringBuilder dirsJson = new StringBuilder("[");
        for (int i = 0; i < shardDirs.size(); i++) {
            if (i > 0) dirsJson.append(",");
            dirsJson.append("\"").append(shardDirs.get(i).replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        }
        dirsJson.append("]");

        byte[] dirsBuf = dirsJson.toString().getBytes(StandardCharsets.UTF_8);
        byte[] sqlBuf = definitionSql.getBytes(StandardCharsets.UTF_8);
        byte[] schemaBuf = schemaJson.getBytes(StandardCharsets.UTF_8);

        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
            try (
                org.apache.arrow.c.ArrowArray cArray = org.apache.arrow.c.ArrowArray.allocateNew(allocator);
                org.apache.arrow.c.ArrowSchema cSchema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
            ) {
                try (Arena arena = Arena.ofConfined()) {
                    // Allocate FFM segments for string args.
                    MemorySegment dirsNative = arena.allocate(dirsBuf.length);
                    dirsNative.copyFrom(MemorySegment.ofArray(dirsBuf));

                    MemorySegment sqlNative = arena.allocate(sqlBuf.length);
                    sqlNative.copyFrom(MemorySegment.ofArray(sqlBuf));

                    MemorySegment schemaNative = arena.allocate(schemaBuf.length);
                    schemaNative.copyFrom(MemorySegment.ofArray(schemaBuf));

                    long rows = MVNativeBridge.queryState(
                        dirsNative, dirsBuf.length,
                        sqlNative, sqlBuf.length,
                        schemaNative, schemaBuf.length,
                        cArray.memoryAddress(),
                        cSchema.memoryAddress()
                    );

                    if (rows < 0) {
                        throw new IOException("df_mv_query_state failed with code: " + rows);
                    }
                }

                // Import via Arrow C-Data.
                try (
                    org.apache.arrow.vector.VectorSchemaRoot batch = org.apache.arrow.c.Data.importVectorSchemaRoot(
                        allocator, cArray, cSchema, null
                    )
                ) {
                    return importArrowBatch(batch, def);
                }
            }
        }
    }

    /**
     * Import Arrow batch into Java-native result structure.
     */
    private MVQueryResult importArrowBatch(
        org.apache.arrow.vector.VectorSchemaRoot batch,
        MVCompiledDefinition def
    ) {
        List<org.apache.arrow.vector.FieldVector> vectors = batch.getFieldVectors();
        List<String> fieldNames = vectors.stream()
            .map(v -> v.getField().getName())
            .toList();

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int row = 0; row < batch.getRowCount(); row++) {
            Map<String, Object> rowMap = new HashMap<>();
            for (int col = 0; col < vectors.size(); col++) {
                Object value = vectors.get(col).getObject(row);
                rowMap.put(fieldNames.get(col), value);
            }
            rows.add(rowMap);
        }
        return new MVQueryResult(fieldNames, rows, batch.getRowCount());
    }

    /**
     * Build a minimal Arrow schema JSON for the source index fields referenced
     * by the MV definition. This is the input schema DataFusion uses for plan
     * surgery (schema-only MemTable).
     */
    private String buildInputSchemaJson(MVCompiledDefinition def, IndexMetadata sourceMeta) {
        // Build from the definition's group keys + aggregate source fields.
        // Map OpenSearch types to Arrow types.
        StringBuilder sb = new StringBuilder("{\"fields\":[");
        boolean first = true;
        for (GroupKey key : def.groupKeys()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("{\"name\":\"").append(escape(key.osFieldPath())).append("\",");
            sb.append("\"data_type\":").append(arrowTypeJson(key.columnType().osType()));
            sb.append(",\"nullable\":true}");
        }
        for (AggregateSpec agg : def.aggregates()) {
            if (agg.sourceField() != null) {
                if (!first) sb.append(",");
                first = false;
                sb.append("{\"name\":\"").append(escape(agg.sourceField())).append("\",");
                // Infer source field type from target mapping type.
                // SUM/MIN/MAX preserve the source type; COUNT always produces long from any input.
                sb.append("\"data_type\":").append(arrowTypeJson(agg.targetMappingType()));
                sb.append(",\"nullable\":true}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String arrowTypeJson(String osType) {
        return switch (osType) {
            case "long", "integer" -> "\"Int64\"";
            case "keyword", "text" -> "\"Utf8\"";
            case "double", "float" -> "\"Float64\"";
            case "date" -> "{\"Timestamp\":[\"Millisecond\",null]}";
            default -> "\"Utf8\""; // fallback
        };
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // ── Result container ─────────────────────────────────────────────────

    public record MVQueryResult(List<String> columns, List<Map<String, Object>> rows, int totalRows) {
        static MVQueryResult empty(MVCompiledDefinition def) {
            // Return empty result with the definition's projection columns.
            return new MVQueryResult(new ArrayList<>(), List.of(), 0);
        }

        public void toXContent(XContentBuilder builder) throws IOException {
            builder.startObject();
            builder.field("total_rows", totalRows);
            builder.startArray("columns");
            for (String col : columns) {
                builder.value(col);
            }
            builder.endArray();
            builder.startArray("rows");
            for (Map<String, Object> row : rows) {
                builder.startObject();
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
        }
    }
}
