/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Stage 3 native schema cross-check for a candidate MV definition.
 *
 * <p>Before an MV definition is accepted (and its target index created), we
 * must confirm that the DataFusion engine will <em>physically</em> produce
 * exactly the Partial-stage state schema the Java descriptor derived. A
 * drifted definition — a source column of the wrong type, an unknown column, a
 * bad time-bucket expression — would otherwise only surface at ingest time,
 * after the target index exists. This validator plans (but never executes) the
 * definition's canonical partial SQL + ordering contract through the native
 * {@code df_mv_validate_definition} FFI against the REAL source schema, then
 * <b>fails closed</b> on any disagreement.</p>
 *
 * <h2>What is compared (fail-closed)</h2>
 * <ol>
 *   <li><b>Arity</b> — native state-field count == {@link MVCompiledDefinition#stateColumnNames()} size.</li>
 *   <li><b>Group-key names + order</b> — the leading {@code groupKeys().size()}
 *       native fields must carry the definition's group-key aliases in order.
 *       (Aggregate state columns are named by DataFusion internally — e.g.
 *       {@code sum(mv_input.m0)[sum]} — so their <em>names</em> are not user
 *       aliases and are intentionally not name-compared; their position and
 *       type are.)</li>
 *   <li><b>Type family, every position</b> — the descriptor-derived arrow token
 *       family must equal the engine's actual token family. Comparison is by
 *       <em>family</em> (integer / float / string / temporal / boolean /
 *       binary), not exact width, because MIN/MAX legitimately preserve a
 *       narrower source width (e.g. {@code MIN} over an {@code Int16} column
 *       yields {@code Int16} state) while COUNT/SUM widen to {@code Int64}.
 *       A cross-family disagreement (e.g. a {@code LONG} key backed by a
 *       {@code keyword} source → {@code Utf8}) is the real bug and is
 *       rejected.</li>
 *   <li><b>Ordering identity hash</b> — {@link MVGroupByOrdering#orderingIdentityHash()}
 *       == native {@code ordering_identity_hash}.</li>
 *   <li><b>Definition hash</b> — {@link MVGroupByOrdering#definitionIdentityHash()}
 *       == native {@code definition_hash}.</li>
 * </ol>
 *
 * <p>The native {@code schema_hash} is surfaced for observability
 * ({@link ValidationResult#nativeSchemaHash()}) but is <b>not</b> used as an
 * authoritative gate: it digests the engine's Rust {@code Debug} rendering of
 * the Arrow schema, which Java cannot reproduce byte-for-byte. The structural
 * name/order + type-family comparison above is strictly stronger.</p>
 *
 * <h2>Testability</h2>
 * <p>The {@link #compare(MVCompiledDefinition, String)} step is pure Java over
 * the native result text and is unit-tested without loading the native library
 * (matching the {@code MvBuildResultLayoutTests}/{@code MVFFIResultContractTests}
 * pattern). End-to-end coverage of the real planning FFI lives in the Rust
 * module tests for {@code validate_definition}.</p>
 */
public final class MVDefinitionValidator {

    private MVDefinitionValidator() {}

    /** One physical state column reported by the engine: name + arrow token. */
    public record StateField(String name, String arrowToken) {
        public StateField {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(arrowToken, "arrowToken");
        }
    }

    /**
     * The outcome of a cross-check.
     *
     * @param ok                true iff there were no mismatches
     * @param nativeStateFields the engine's ACTUAL Partial-stage state schema,
     *                          in physical order (empty when the native call
     *                          itself was rejected)
     * @param mismatches        precise, human-readable failure reasons (empty
     *                          when {@code ok})
     * @param nativeSchemaHash  the engine's opaque schema digest (observability)
     */
    public record ValidationResult(boolean ok, List<StateField> nativeStateFields, List<String> mismatches, long nativeSchemaHash) {
        public ValidationResult {
            nativeStateFields = List.copyOf(nativeStateFields);
            mismatches = List.copyOf(mismatches);
        }

        /** Rejection outcome with a single reason and no schema. */
        static ValidationResult rejected(String reason) {
            return new ValidationResult(false, List.of(), List.of(reason), 0L);
        }
    }

    // ── Entry points ─────────────────────────────────────────────────────

    /**
     * Validate a descriptor against a source field→OpenSearch-mapping-type map.
     * Compiles the descriptor, encodes the source schema, calls the native
     * cross-check, and fail-closed compares. This is the entry point the
     * Stage&nbsp;5 {@code POST /_mv/_validate} endpoint calls.
     *
     * @param descriptor    the candidate definition descriptor
     * @param sourceOsTypes source field name → OpenSearch mapping type (e.g.
     *                      {@code "long"}, {@code "keyword"}, {@code "date"})
     */
    public static ValidationResult validate(MVDefinitionDescriptor descriptor, Map<String, String> sourceOsTypes) {
        Objects.requireNonNull(descriptor, "descriptor");
        final MVCompiledDefinition def;
        try {
            def = MVCompiledDefinition.fromDescriptor(descriptor);
        } catch (RuntimeException e) {
            return ValidationResult.rejected("descriptor compile failed: " + e.getMessage());
        }
        return validate(def, sourceOsTypes);
    }

    /**
     * Validate an already-compiled definition against a source field→OS-type
     * map.
     */
    public static ValidationResult validate(MVCompiledDefinition def, Map<String, String> sourceOsTypes) {
        Objects.requireNonNull(def, "def");
        Objects.requireNonNull(sourceOsTypes, "sourceOsTypes");

        String schemaWire = buildSourceSchemaWire(sourceOsTypes);
        String sql = def.buildPartialSql(MVConstants.INPUT_TABLE);
        MVGroupByOrdering.FFIArrays ffi = def.groupByOrdering().toFFIArrays();

        final String nativeText;
        try {
            nativeText = MVNativeBridge.validateDefinition(
                schemaWire,
                MVConstants.INPUT_TABLE,
                sql,
                ffi.indices(),
                ffi.directions(),
                ffi.nulls()
            );
        } catch (RuntimeException e) {
            // A native planning rejection (unknown column, type mismatch,
            // unparseable SQL) surfaces here with a precise message.
            return ValidationResult.rejected("native validation rejected definition: " + e.getMessage());
        }
        return compare(def, nativeText);
    }

    // ── Pure-Java comparison (unit-tested without native) ────────────────

    /**
     * Fail-closed compare the descriptor-derived expectation against the native
     * result text produced by {@code df_mv_validate_definition}. Package-private
     * and pure Java so it is unit-testable without the native library.
     *
     * @param def        the compiled definition (source of the expected layout)
     * @param nativeText the native result document (see
     *                   {@link MVNativeBridge#validateDefinition})
     */
    static ValidationResult compare(MVCompiledDefinition def, String nativeText) {
        final NativeResult nr;
        try {
            nr = parseNativeResult(nativeText);
        } catch (RuntimeException e) {
            return ValidationResult.rejected("unparseable native validation result: " + e.getMessage());
        }

        List<String> mismatches = new ArrayList<>();
        List<String> expectedNames = def.stateColumnNames();
        List<String> expectedTokens = expectedArrowTokens(def);
        int numGroupKeys = def.groupKeys().size();

        // 1. Arity.
        if (nr.fields.size() != expectedNames.size()) {
            mismatches.add(
                String.format(
                    Locale.ROOT,
                    "state field count mismatch: expected %d %s but engine produced %d %s",
                    expectedNames.size(),
                    expectedNames,
                    nr.fields.size(),
                    nativeNames(nr.fields)
                )
            );
        }

        int n = Math.min(expectedNames.size(), nr.fields.size());
        for (int i = 0; i < n; i++) {
            StateField nf = nr.fields.get(i);

            // 2. Group-key name + order (aggregate names are engine-internal).
            if (i < numGroupKeys && expectedNames.get(i).equals(nf.name()) == false) {
                mismatches.add(
                    String.format(
                        Locale.ROOT,
                        "group key position %d: expected name [%s] but engine produced [%s]",
                        i,
                        expectedNames.get(i),
                        nf.name()
                    )
                );
            }

            // 3. Type family (every position).
            String expTok = expectedTokens.get(i);
            String expFam = arrowFamily(expTok);
            String natFam = arrowFamily(nf.arrowToken());
            if (expFam.equals(natFam) == false) {
                String label = i < numGroupKeys ? expectedNames.get(i) : expectedNames.get(i) + " (aggregate state)";
                mismatches.add(
                    String.format(
                        Locale.ROOT,
                        "field [%s] (position %d): expected type family %s(%s) but engine produces %s(%s)",
                        label,
                        i,
                        expFam,
                        expTok,
                        natFam,
                        nf.arrowToken()
                    )
                );
            }
        }

        // 4. Ordering identity hash.
        long expOrderingHash = def.groupByOrdering().orderingIdentityHash();
        if (expOrderingHash != nr.orderingIdentityHash) {
            mismatches.add(
                String.format(
                    Locale.ROOT,
                    "ordering identity hash mismatch: expected 0x%016x but native returned 0x%016x",
                    expOrderingHash,
                    nr.orderingIdentityHash
                )
            );
        }

        // 5. Definition hash.
        long expDefinitionHash = def.groupByOrdering().definitionIdentityHash();
        if (expDefinitionHash != nr.definitionHash) {
            mismatches.add(
                String.format(
                    Locale.ROOT,
                    "definition hash mismatch: expected 0x%016x but native returned 0x%016x",
                    expDefinitionHash,
                    nr.definitionHash
                )
            );
        }

        return new ValidationResult(mismatches.isEmpty(), nr.fields, mismatches, nr.schemaHash);
    }

    // ── Type mapping (mirrors the parquet data-format arrow types) ───────

    /**
     * Map an OpenSearch mapping type to the canonical arrow token, mirroring
     * the arrow types the parquet data-format produces for source columns (see
     * {@code *ParquetField.getArrowType()} / {@code ArrowSchemaBuilder}).
     *
     * @return the arrow token, or {@code null} when the OS type has no columnar
     *         arrow representation (such fields are simply omitted from the
     *         source schema — if a definition references one, native planning
     *         fails closed with an "unknown column" error)
     */
    public static String osTypeToArrowToken(String osType) {
        if (osType == null) {
            return null;
        }
        return switch (osType.toLowerCase(Locale.ROOT)) {
            case "long" -> "int64";
            case "integer" -> "int32";
            case "short" -> "int16";
            case "byte" -> "int8";
            case "unsigned_long" -> "uint64";
            case "double", "scaled_float" -> "float64";
            case "float" -> "float32";
            case "half_float" -> "float16";
            case "boolean" -> "bool";
            case "date", "date_nanos" -> "timestamp_ms";
            case "keyword", "text", "ip", "match_only_text", "wildcard", "constant_keyword" -> "utf8";
            case "binary" -> "binary";
            default -> null;
        };
    }

    /** The arrow-token family used for width-tolerant type comparison. */
    static String arrowFamily(String token) {
        return switch (token) {
            case "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64" -> "integer";
            case "float16", "float32", "float64" -> "float";
            case "utf8" -> "string";
            case "bool" -> "boolean";
            case "timestamp_ms", "timestamp_s", "timestamp_us", "timestamp_ns", "date32", "date64" -> "temporal";
            case "binary" -> "binary";
            default -> token;
        };
    }

    /** Expected arrow token per state column, in physical order. */
    static List<String> expectedArrowTokens(MVCompiledDefinition def) {
        List<String> tokens = new ArrayList<>();
        for (GroupKey key : def.groupKeys()) {
            tokens.add(columnTypeToken(key.columnType()));
        }
        for (AggregateSpec agg : def.aggregates()) {
            for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                tokens.add(physicalTypeToken(sc.physicalType()));
            }
        }
        return tokens;
    }

    private static String columnTypeToken(GroupKey.ColumnType type) {
        return switch (type) {
            case LONG -> "int64";
            case INTEGER -> "int32";
            case DOUBLE -> "float64";
            case KEYWORD -> "utf8";
        };
    }

    private static String physicalTypeToken(String physicalType) {
        return switch (physicalType.toLowerCase(Locale.ROOT)) {
            case "long" -> "int64";
            case "double" -> "float64";
            case "integer" -> "int32";
            case "keyword" -> "utf8";
            default -> physicalType.toLowerCase(Locale.ROOT);
        };
    }

    // ── Wire encoding + parsing ──────────────────────────────────────────

    /**
     * Encode the source schema as newline-separated {@code name\tarrow_token}
     * records. Fields whose OS type has no arrow representation are omitted
     * (see {@link #osTypeToArrowToken(String)}).
     */
    static String buildSourceSchemaWire(Map<String, String> sourceOsTypes) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> e : sourceOsTypes.entrySet()) {
            String token = osTypeToArrowToken(e.getValue());
            if (token == null) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(e.getKey()).append('\t').append(token);
        }
        return sb.toString();
    }

    /** Parsed native validation result document. */
    private record NativeResult(List<StateField> fields, long schemaHash, long orderingIdentityHash, long definitionHash) {}

    private static NativeResult parseNativeResult(String text) {
        Objects.requireNonNull(text, "native result text");
        List<StateField> fields = new ArrayList<>();
        long schemaHash = 0;
        long orderingIdentityHash = 0;
        long definitionHash = 0;
        boolean sawSchema = false;
        boolean sawOrdering = false;
        boolean sawDefinition = false;

        for (String line : text.split("\n")) {
            if (line.isBlank()) {
                continue;
            }
            String[] parts = line.split("\t");
            switch (parts[0]) {
                case "schema_hash" -> {
                    requireArity(parts, 2, line);
                    schemaHash = Long.parseUnsignedLong(parts[1].trim());
                    sawSchema = true;
                }
                case "ordering_identity_hash" -> {
                    requireArity(parts, 2, line);
                    orderingIdentityHash = Long.parseUnsignedLong(parts[1].trim());
                    sawOrdering = true;
                }
                case "definition_hash" -> {
                    requireArity(parts, 2, line);
                    definitionHash = Long.parseUnsignedLong(parts[1].trim());
                    sawDefinition = true;
                }
                case "field" -> {
                    requireArity(parts, 3, line);
                    fields.add(new StateField(parts[1], parts[2].trim()));
                }
                default -> throw new IllegalArgumentException("unknown record type [" + parts[0] + "] in native result");
            }
        }
        if (sawSchema == false || sawOrdering == false || sawDefinition == false) {
            throw new IllegalArgumentException("native result missing one or more required hash records");
        }
        return new NativeResult(fields, schemaHash, orderingIdentityHash, definitionHash);
    }

    private static void requireArity(String[] parts, int expected, String line) {
        if (parts.length < expected) {
            throw new IllegalArgumentException("malformed record [" + line + "]: expected " + expected + " tab-separated fields");
        }
    }

    private static List<String> nativeNames(List<StateField> fields) {
        List<String> names = new ArrayList<>(fields.size());
        for (StateField f : fields) {
            names.add(f.name());
        }
        return names;
    }
}
