/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Defect #23 at the production seam: {@link MVIndexingEngine#isMergeEligible}
 * must reject every merge candidate while a build round holds the shard's
 * build-pressure claim, and admit candidates again once the claim clears.
 * <p>
 * The full retry chain this predicate participates in: the builder marks the
 * claim per round (clearing it before the final round's publish), BOTH
 * selection paths filter through this predicate
 * ({@code DataFormatAwareMergePolicy#eligible} — covered by
 * {@code DataFormatAwareMergePolicyTests}), a rejected candidate is never
 * registered so it holds no merging claim, and the publication trigger
 * re-runs selection ({@code publishDerivedArtifact ->
 * triggerPossibleMerges}) — so "rejected during, selected after" needs no
 * bespoke retry queue.
 */
public class MVMergeEligibilityBuildPressureTests extends OpenSearchTestCase {

    private static final String TARGET_INDEX = "mv-target";

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MVBuildActivity.clearForTests();
        MVTargetCursorLedger.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        MVBuildActivity.clearForTests();
        MVTargetCursorLedger.clearForTests();
        super.tearDown();
    }

    private ShardPath shardPath(String indexName, int shardId) throws Exception {
        Path tempDir = createTempDir();
        Path dataPath = tempDir.resolve("_na_").resolve(Integer.toString(shardId));
        Files.createDirectories(dataPath);
        return new ShardPath(false, dataPath, dataPath, new ShardId(indexName, "_na_", shardId));
    }

    /** Target-form engine: mv_state format, no ship targets — the gated shape. */
    private MVIndexingEngine targetEngine(String indexName, int shardId) throws Exception {
        return new MVIndexingEngine(
            shardPath(indexName, shardId),
            indexName,
            MVDefinitionSpec.SOURCE,
            MVStateDataFormat.INSTANCE,
            "payments",
            List.of(),
            () -> null,
            () -> null,
            false,
            () -> TargetRoutingSnapshot.EMPTY
        );
    }

    public void testCandidatesRejectedDuringActiveBuildSelectedAfter() throws Exception {
        MVIndexingEngine engine = targetEngine(TARGET_INDEX, 0);

        assertTrue("baseline: no build pressure, ledger clean — eligible", engine.isMergeEligible(List.of()));

        // Build round enters its memory-holding section.
        MVBuildActivity.markActive(TARGET_INDEX, 0);
        assertFalse("candidates must be rejected while a build round is active", engine.isMergeEligible(List.of()));

        // Round releases its claim (final round clears before publish; the
        // publication trigger then re-runs selection against this predicate).
        MVBuildActivity.clearActive(TARGET_INDEX, 0);
        assertTrue("candidates must be admitted again after the build round", engine.isMergeEligible(List.of()));
    }

    public void testNestedClaimsMustAllResolveBeforeAdmission() throws Exception {
        MVIndexingEngine engine = targetEngine(TARGET_INDEX, 0);

        MVBuildActivity.markActive(TARGET_INDEX, 0);
        MVBuildActivity.markActive(TARGET_INDEX, 0);
        MVBuildActivity.clearActive(TARGET_INDEX, 0);
        assertFalse("one claim still held — merges stay stalled", engine.isMergeEligible(List.of()));
        MVBuildActivity.clearActive(TARGET_INDEX, 0);
        assertTrue(engine.isMergeEligible(List.of()));
    }

    public void testPressureIsPerShard() throws Exception {
        MVIndexingEngine shard0 = targetEngine(TARGET_INDEX, 0);
        MVIndexingEngine shard1 = targetEngine(TARGET_INDEX, 1);

        MVBuildActivity.markActive(TARGET_INDEX, 0);
        assertFalse(shard0.isMergeEligible(List.of()));
        assertTrue("build pressure on shard 0 must not stall shard 1", shard1.isMergeEligible(List.of()));
        MVBuildActivity.clearActive(TARGET_INDEX, 0);
    }

    public void testShipPathSourceEngineIsNeverBuildGated() throws Exception {
        // Ship-path SOURCE engine (non-empty ship targets): merges on the
        // source are not the MV build's concern — always eligible.
        MVIndexingEngine sourceEngine = new MVIndexingEngine(
            shardPath("source-index", 0),
            "source-index",
            MVDefinitionSpec.SOURCE,
            MVDataFormat.INSTANCE,
            "payments",
            List.of(TARGET_INDEX),
            () -> null,
            () -> null,
            false,
            () -> TargetRoutingSnapshot.EMPTY
        );
        MVBuildActivity.markActive("source-index", 0);
        assertTrue("ship-path source engines are never build-gated", sourceEngine.isMergeEligible(List.of()));
        MVBuildActivity.clearActive("source-index", 0);
    }

    public void testNonMvStateFormatIsNeverBuildGated() throws Exception {
        MVIndexingEngine embedded = new MVIndexingEngine(
            shardPath(TARGET_INDEX, 0),
            TARGET_INDEX,
            MVDefinitionSpec.SOURCE,
            MVDataFormat.INSTANCE,
            "payments",
            List.of(),
            () -> null,
            () -> null,
            false,
            () -> TargetRoutingSnapshot.EMPTY
        );
        MVBuildActivity.markActive(TARGET_INDEX, 0);
        assertTrue("non-mv_state formats are never build-gated", embedded.isMergeEligible(List.of()));
        MVBuildActivity.clearActive(TARGET_INDEX, 0);
    }
}
