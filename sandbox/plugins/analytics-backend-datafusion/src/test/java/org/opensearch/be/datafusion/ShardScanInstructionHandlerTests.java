/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.analytics.backend.ShardScanExecutionContext;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.IndexReaderProvider.Reader;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ShardScanInstructionHandler}'s derived-MV dispatch
 * decision and state file/field resolution. These cover the pure routing and
 * catalog/settings resolution logic; the native session creation is exercised
 * by the analytics-engine integration tests.
 */
public class ShardScanInstructionHandlerTests extends OpenSearchTestCase {

    // ── Dispatch decision ────────────────────────────────────────────────

    public void testIsDerivedMVTargetTrueWhenSettingIsMaterializedView() {
        Settings settings = Settings.builder().put("index.derived.data_format", "materialized_view").build();
        assertTrue(ShardScanInstructionHandler.isDerivedMVTarget(settings));
    }

    public void testIsDerivedMVTargetFalseWhenSettingAbsent() {
        assertFalse(ShardScanInstructionHandler.isDerivedMVTarget(Settings.EMPTY));
    }

    public void testIsDerivedMVTargetFalseForOtherCategory() {
        Settings settings = Settings.builder().put("index.derived.data_format", "something_else").build();
        assertFalse(ShardScanInstructionHandler.isDerivedMVTarget(settings));
    }

    // ── State field resolution ───────────────────────────────────────────

    public void testResolveMVStateFieldsReturnsOrderedList() {
        Settings settings = Settings.builder()
            .putList("index.mv.state_fields", "service", "region", "cnt", "lat_sum")
            .build();
        ShardScanExecutionContext ctx = contextWithSettings(settings);
        List<String> fields = ShardScanInstructionHandler.resolveMVStateFields(ctx);
        assertEquals(List.of("service", "region", "cnt", "lat_sum"), fields);
    }

    public void testResolveMVStateFieldsThrowsWhenAbsent() {
        ShardScanExecutionContext ctx = contextWithSettings(Settings.EMPTY);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ShardScanInstructionHandler.resolveMVStateFields(ctx)
        );
        assertTrue(e.getMessage().contains("index.mv.state_fields"));
    }

    // ── State file resolution ────────────────────────────────────────────

    public void testResolveMVStateFilesFlattensAndSorts() {
        // Two file sets in two directories; files() is an unordered set — the
        // result must be the sorted union of absolute "<dir>/<file>" paths.
        WriterFileSet setA = new WriterFileSet(
            "/data/mv_hydrated/s0",
            0L,
            Set.of("_mv_partial.s0.t1.g1.bbb.parquet", "_mv_partial.s0.t1.g0.aaa.parquet"),
            2L,
            0L
        );
        WriterFileSet setB = new WriterFileSet("/data/mv_hydrated/s1", 0L, Set.of("_mv_partial.s1.t1.g0.ccc.parquet"), 1L, 0L);

        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSearchableFiles("mv_state")).thenReturn(List.of(setA, setB));

        ShardScanExecutionContext ctx = contextWithSnapshot(snapshot);
        List<String> files = ShardScanInstructionHandler.resolveMVStateFiles(ctx);

        assertEquals(
            List.of(
                "/data/mv_hydrated/s0/_mv_partial.s0.t1.g0.aaa.parquet",
                "/data/mv_hydrated/s0/_mv_partial.s0.t1.g1.bbb.parquet",
                "/data/mv_hydrated/s1/_mv_partial.s1.t1.g0.ccc.parquet"
            ),
            files
        );
    }

    public void testResolveMVStateFilesUsesMvStateArtifactName() {
        assertEquals("mv_state", ShardScanInstructionHandler.MV_STATE_ARTIFACT_NAME);
    }

    public void testResolveMVStateFilesThrowsWhenNoStateFiles() {
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSearchableFiles("mv_state")).thenReturn(List.of());
        when(snapshot.getSearchableFiles("parquet")).thenReturn(List.of());
        ShardScanExecutionContext ctx = contextWithSnapshot(snapshot);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> ShardScanInstructionHandler.resolveMVStateFiles(ctx)
        );
        assertTrue(e.getMessage().contains("mv_state"));
        assertTrue(e.getMessage().contains("parquet"));
    }

    public void testResolveMVStateFilesFallsBackToPullParquetGenerations() {
        // Pull-based MV: the builder publishes state as stock parquet generations of
        // the derived target (MVConstants.STATE_ARTIFACT_FORMAT = "parquet"); no
        // mv_state sets exist. The derived target holds no raw rows, so its parquet
        // generations are the state files.
        WriterFileSet gen1 = new WriterFileSet("/data/idx/0/parquet", 1L, Set.of("_parquet_file_generation_mv_1.parquet"), 1L, 0L);
        WriterFileSet gen2 = new WriterFileSet("/data/idx/0/parquet", 2L, Set.of("_parquet_file_generation_mv_2.parquet"), 1L, 0L);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSearchableFiles("mv_state")).thenReturn(List.of());
        when(snapshot.getSearchableFiles("parquet")).thenReturn(List.of(gen2, gen1));
        ShardScanExecutionContext ctx = contextWithSnapshot(snapshot);

        List<String> files = ShardScanInstructionHandler.resolveMVStateFiles(ctx);
        assertEquals(
            List.of("/data/idx/0/parquet/_parquet_file_generation_mv_1.parquet", "/data/idx/0/parquet/_parquet_file_generation_mv_2.parquet"),
            files
        );
    }

    public void testResolveMVStateFilesPrefersMvStateOverParquet() {
        WriterFileSet hydrated = new WriterFileSet("/data/idx/0/mv_state", 0L, Set.of("_mv_partial.s0.t1.g0.aaa.parquet"), 1L, 0L);
        WriterFileSet parquet = new WriterFileSet("/data/idx/0/parquet", 1L, Set.of("_parquet_file_generation_1.parquet"), 1L, 0L);
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getSearchableFiles("mv_state")).thenReturn(List.of(hydrated));
        when(snapshot.getSearchableFiles("parquet")).thenReturn(List.of(parquet));
        ShardScanExecutionContext ctx = contextWithSnapshot(snapshot);

        assertEquals(List.of("/data/idx/0/mv_state/_mv_partial.s0.t1.g0.aaa.parquet"), ShardScanInstructionHandler.resolveMVStateFiles(ctx));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static ShardScanExecutionContext contextWithSettings(Settings settings) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("mv_target", settings);
        ShardScanExecutionContext ctx = mock(ShardScanExecutionContext.class);
        when(ctx.getIndexSettings()).thenReturn(indexSettings);
        return ctx;
    }

    private static ShardScanExecutionContext contextWithSnapshot(CatalogSnapshot snapshot) {
        Reader reader = mock(Reader.class);
        when(reader.catalogSnapshot()).thenReturn(snapshot);
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("mv_target", Settings.EMPTY);
        ShardScanExecutionContext ctx = mock(ShardScanExecutionContext.class);
        when(ctx.getReader()).thenReturn(reader);
        when(ctx.getIndexSettings()).thenReturn(indexSettings);
        return ctx;
    }
}
