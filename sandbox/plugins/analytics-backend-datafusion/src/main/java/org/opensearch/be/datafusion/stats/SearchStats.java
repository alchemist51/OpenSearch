/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SearchStats implements Writeable, ToXContentFragment {

    public final long elapsedComputeMs;
    public final long delegationCalls;
    public final long rgProcessed;
    public final long rgSkipped;
    public final long prefetchWaitTimeMs;
    public final long prefetchWaitCount;
    public final long onBatchMaskTimeMs;
    public final long parquetPollTimeMs;
    public final long buildMaskTimeMs;
    public final long filterRecordBatchTimeMs;
    public final long parquetTimeMs;
    public final long listingTableScan;
    public final long singleCollectorScan;
    public final long bitmapTreeScan;
    public final long maskSliceTimeMs;
    public final long projectionFixupTimeMs;
    public final long coalesceTimeMs;
    public final long coalesceDrainTimeMs;
    public final long rgSetupTimeMs;
    public final long indexDispatchTimeMs;
    public final long pollInnerTimeMs;
    public final long indexTimeMs;
    public final long partitionWallClockMs;
    public final long outputRows;
    public final long batchesProduced;
    public final long parquetBatchesReceived;

    public SearchStats(
        long elapsedComputeMs,
        long delegationCalls,
        long rgProcessed,
        long rgSkipped,
        long prefetchWaitTimeMs,
        long prefetchWaitCount,
        long onBatchMaskTimeMs,
        long parquetPollTimeMs,
        long buildMaskTimeMs,
        long filterRecordBatchTimeMs,
        long parquetTimeMs,
        long listingTableScan,
        long singleCollectorScan,
        long bitmapTreeScan,
        long maskSliceTimeMs,
        long projectionFixupTimeMs,
        long coalesceTimeMs,
        long coalesceDrainTimeMs,
        long rgSetupTimeMs,
        long indexDispatchTimeMs,
        long pollInnerTimeMs,
        long indexTimeMs,
        long partitionWallClockMs,
        long outputRows,
        long batchesProduced,
        long parquetBatchesReceived
    ) {
        this.elapsedComputeMs = elapsedComputeMs;
        this.delegationCalls = delegationCalls;
        this.rgProcessed = rgProcessed;
        this.rgSkipped = rgSkipped;
        this.prefetchWaitTimeMs = prefetchWaitTimeMs;
        this.prefetchWaitCount = prefetchWaitCount;
        this.onBatchMaskTimeMs = onBatchMaskTimeMs;
        this.parquetPollTimeMs = parquetPollTimeMs;
        this.buildMaskTimeMs = buildMaskTimeMs;
        this.filterRecordBatchTimeMs = filterRecordBatchTimeMs;
        this.parquetTimeMs = parquetTimeMs;
        this.listingTableScan = listingTableScan;
        this.singleCollectorScan = singleCollectorScan;
        this.bitmapTreeScan = bitmapTreeScan;
        this.maskSliceTimeMs = maskSliceTimeMs;
        this.projectionFixupTimeMs = projectionFixupTimeMs;
        this.coalesceTimeMs = coalesceTimeMs;
        this.coalesceDrainTimeMs = coalesceDrainTimeMs;
        this.rgSetupTimeMs = rgSetupTimeMs;
        this.indexDispatchTimeMs = indexDispatchTimeMs;
        this.pollInnerTimeMs = pollInnerTimeMs;
        this.indexTimeMs = indexTimeMs;
        this.partitionWallClockMs = partitionWallClockMs;
        this.outputRows = outputRows;
        this.batchesProduced = batchesProduced;
        this.parquetBatchesReceived = parquetBatchesReceived;
    }

    public SearchStats(StreamInput in) throws IOException {
        this.elapsedComputeMs = in.readVLong();
        this.delegationCalls = in.readVLong();
        this.rgProcessed = in.readVLong();
        this.rgSkipped = in.readVLong();
        this.prefetchWaitTimeMs = in.readVLong();
        this.prefetchWaitCount = in.readVLong();
        this.onBatchMaskTimeMs = in.readVLong();
        this.parquetPollTimeMs = in.readVLong();
        this.buildMaskTimeMs = in.readVLong();
        this.filterRecordBatchTimeMs = in.readVLong();
        this.parquetTimeMs = in.readVLong();
        this.listingTableScan = in.readVLong();
        this.singleCollectorScan = in.readVLong();
        this.bitmapTreeScan = in.readVLong();
        this.maskSliceTimeMs = in.readVLong();
        this.projectionFixupTimeMs = in.readVLong();
        this.coalesceTimeMs = in.readVLong();
        this.coalesceDrainTimeMs = in.readVLong();
        this.rgSetupTimeMs = in.readVLong();
        this.indexDispatchTimeMs = in.readVLong();
        this.pollInnerTimeMs = in.readVLong();
        this.indexTimeMs = in.readVLong();
        this.partitionWallClockMs = in.readVLong();
        this.outputRows = in.readVLong();
        this.batchesProduced = in.readVLong();
        this.parquetBatchesReceived = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(elapsedComputeMs);
        out.writeVLong(delegationCalls);
        out.writeVLong(rgProcessed);
        out.writeVLong(rgSkipped);
        out.writeVLong(prefetchWaitTimeMs);
        out.writeVLong(prefetchWaitCount);
        out.writeVLong(onBatchMaskTimeMs);
        out.writeVLong(parquetPollTimeMs);
        out.writeVLong(buildMaskTimeMs);
        out.writeVLong(filterRecordBatchTimeMs);
        out.writeVLong(parquetTimeMs);
        out.writeVLong(listingTableScan);
        out.writeVLong(singleCollectorScan);
        out.writeVLong(bitmapTreeScan);
        out.writeVLong(maskSliceTimeMs);
        out.writeVLong(projectionFixupTimeMs);
        out.writeVLong(coalesceTimeMs);
        out.writeVLong(coalesceDrainTimeMs);
        out.writeVLong(rgSetupTimeMs);
        out.writeVLong(indexDispatchTimeMs);
        out.writeVLong(pollInnerTimeMs);
        out.writeVLong(indexTimeMs);
        out.writeVLong(partitionWallClockMs);
        out.writeVLong(outputRows);
        out.writeVLong(batchesProduced);
        out.writeVLong(parquetBatchesReceived);
    }

    /**
     * Sum of all timed sub-phases that contribute to per-query wall-clock.
     * Used to compute the residual = partition_wall_clock - timedSum.
     */
    public long timedPhasesSumMs() {
        return parquetPollTimeMs
            + onBatchMaskTimeMs
            + buildMaskTimeMs
            + filterRecordBatchTimeMs
            + maskSliceTimeMs
            + projectionFixupTimeMs
            + coalesceTimeMs
            + coalesceDrainTimeMs
            + rgSetupTimeMs
            + parquetTimeMs
            + indexDispatchTimeMs
            + indexTimeMs
            + prefetchWaitTimeMs;
    }

    /**
     * Wall-clock time NOT attributed to a named sub-phase. Computed as
     * {@code partitionWallClockMs - timedPhasesSumMs()}, clamped at 0.
     */
    public long residualMs() {
        long sum = timedPhasesSumMs();
        return partitionWallClockMs > sum ? partitionWallClockMs - sum : 0;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("search_stats");
        builder.field("elapsed_compute_ms", elapsedComputeMs);
        builder.field("delegation_calls", delegationCalls);
        builder.field("rg_processed", rgProcessed);
        builder.field("rg_skipped", rgSkipped);
        builder.field("prefetch_wait_time_ms", prefetchWaitTimeMs);
        builder.field("prefetch_wait_count", prefetchWaitCount);
        builder.field("on_batch_mask_time_ms", onBatchMaskTimeMs);
        builder.field("parquet_poll_time_ms", parquetPollTimeMs);
        builder.field("build_mask_time_ms", buildMaskTimeMs);
        builder.field("filter_record_batch_time_ms", filterRecordBatchTimeMs);
        builder.field("parquet_time_ms", parquetTimeMs);
        builder.field("listing_table_scan", listingTableScan);
        builder.field("single_collector_scan", singleCollectorScan);
        builder.field("bitmap_tree_scan", bitmapTreeScan);
        builder.field("mask_slice_time_ms", maskSliceTimeMs);
        builder.field("projection_fixup_time_ms", projectionFixupTimeMs);
        builder.field("coalesce_time_ms", coalesceTimeMs);
        builder.field("coalesce_drain_time_ms", coalesceDrainTimeMs);
        builder.field("rg_setup_time_ms", rgSetupTimeMs);
        builder.field("index_dispatch_time_ms", indexDispatchTimeMs);
        builder.field("poll_inner_time_ms", pollInnerTimeMs);
        builder.field("index_time_ms", indexTimeMs);
        builder.field("partition_wall_clock_ms", partitionWallClockMs);
        builder.field("output_rows", outputRows);
        builder.field("batches_produced", batchesProduced);
        builder.field("parquet_batches_received", parquetBatchesReceived);
        builder.field("timed_phases_sum_ms", timedPhasesSumMs());
        builder.field("residual_ms", residualMs());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchStats that = (SearchStats) o;
        return elapsedComputeMs == that.elapsedComputeMs
            && delegationCalls == that.delegationCalls
            && rgProcessed == that.rgProcessed
            && rgSkipped == that.rgSkipped
            && prefetchWaitTimeMs == that.prefetchWaitTimeMs
            && prefetchWaitCount == that.prefetchWaitCount
            && onBatchMaskTimeMs == that.onBatchMaskTimeMs
            && parquetPollTimeMs == that.parquetPollTimeMs
            && buildMaskTimeMs == that.buildMaskTimeMs
            && filterRecordBatchTimeMs == that.filterRecordBatchTimeMs
            && parquetTimeMs == that.parquetTimeMs
            && listingTableScan == that.listingTableScan
            && singleCollectorScan == that.singleCollectorScan
            && bitmapTreeScan == that.bitmapTreeScan
            && maskSliceTimeMs == that.maskSliceTimeMs
            && projectionFixupTimeMs == that.projectionFixupTimeMs
            && coalesceTimeMs == that.coalesceTimeMs
            && coalesceDrainTimeMs == that.coalesceDrainTimeMs
            && rgSetupTimeMs == that.rgSetupTimeMs
            && indexDispatchTimeMs == that.indexDispatchTimeMs
            && pollInnerTimeMs == that.pollInnerTimeMs
            && indexTimeMs == that.indexTimeMs
            && partitionWallClockMs == that.partitionWallClockMs
            && outputRows == that.outputRows
            && batchesProduced == that.batchesProduced
            && parquetBatchesReceived == that.parquetBatchesReceived;
    }

    @Override
    public int hashCode() {
        int h = Objects.hash(
            elapsedComputeMs,
            delegationCalls,
            rgProcessed,
            rgSkipped,
            prefetchWaitTimeMs,
            prefetchWaitCount,
            onBatchMaskTimeMs,
            parquetPollTimeMs,
            buildMaskTimeMs,
            filterRecordBatchTimeMs,
            parquetTimeMs,
            listingTableScan,
            singleCollectorScan,
            bitmapTreeScan
        );
        return 31 * h
            + Objects.hash(
                maskSliceTimeMs,
                projectionFixupTimeMs,
                coalesceTimeMs,
                coalesceDrainTimeMs,
                rgSetupTimeMs,
                indexDispatchTimeMs,
                pollInnerTimeMs,
                indexTimeMs,
                partitionWallClockMs,
                outputRows,
                batchesProduced,
                parquetBatchesReceived
            );
    }
}
