/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use crate::jemalloc_monitor::AllocationMonitor;
use datafusion::common::DataFusionError;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use std::result;
use std::sync::Arc;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

#[derive(Debug)]
pub struct AllocationMonitoringMemoryPool {
    monitor: Arc<AllocationMonitor>
}

impl AllocationMonitoringMemoryPool {
    /// Create a new pool that can allocate up to `pool_size` bytes
    pub fn new(pool_size: usize) -> Self {
        println!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            monitor: Arc::new(AllocationMonitor::try_new(pool_size).unwrap())
        }
    }
}

impl MemoryPool for AllocationMonitoringMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.monitor.reserve(additional);
    }

    fn shrink(&self, _reservation: &MemoryReservation, _shrink: usize) {
        // We don't need to shrink since we are tracking using the jemalloc
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.monitor.try_reserve(additional).map_err(|e| match e {
            _ => {
                insufficient_capacity_err(reservation, additional, self.monitor.limit() - self.monitor.current_used())
            }
        })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.monitor.current_used()
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.monitor.limit())
    }
}


#[inline(always)]
fn insufficient_capacity_err(
    reservation: &MemoryReservation,
    additional: usize,
    available: usize,
) -> DataFusionError {
    DataFusionError::ResourcesExhausted(format!(
        "Failed to allocate additional {} bytes for {} with {} bytes already allocated for this reservation - {} bytes remain available for the total pool",
        additional, reservation.consumer().name(), reservation.size(), available
    ))
}
