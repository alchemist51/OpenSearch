/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use datafusion::common::DataFusionError;
use snafu::{ResultExt, Snafu};
use vectorized_exec_spi::log_debug;

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum AllocationMonitorError {
    #[snafu(display("Error getting jemalloc stats: {}", source))]
    Jemalloc { source: tikv_jemalloc_ctl::Error },

    #[snafu(display("Heap exhausted"))]
    HeapExhausted,
}


/// Wrapper around MonitoredMemoryPool providing access to memory monitoring capabilities.
#[derive(Debug)]
pub struct CustomMemoryPool {
    memory_pool: Arc<MonitoredMemoryPool>
}

impl CustomMemoryPool {
    pub fn new(memory_pool: Arc<MonitoredMemoryPool>) -> Self {
        Self { memory_pool }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.memory_pool.get_monitor()
    }

    pub fn get_memory_pool(&self) -> Arc<dyn MemoryPool> {
        self.memory_pool.clone()
    }
}

/// Tracks current and peak memory usage atomically.
#[derive(Debug, Default)]
pub(crate) struct Monitor {
    pub(crate) value: AtomicUsize,
    pub(crate) max: AtomicUsize,
}

impl Monitor {
    pub(crate) fn max(&self) -> usize {
        self.max.load(Ordering::Relaxed)
    }

    fn grow(&self, amount: usize) {
        let old = self.value.fetch_add(amount, Ordering::Relaxed);
        self.max.fetch_max(old + amount, Ordering::Relaxed);
    }

    fn shrink(&self, amount: usize) {
        self.value.fetch_sub(amount, Ordering::Relaxed);
    }

    fn get_current_val(&self) -> usize {
        self.value.load(Ordering::Relaxed)
    }
}

/// MemoryPool implementation that wraps another pool and tracks memory usage via Monitor.
#[derive(Debug)]
pub struct MonitoredMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<Monitor>,
}

impl MonitoredMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<Monitor>) -> Self {
        Self { inner, monitor }
    }

    pub fn get_monitor(&self) -> Arc<Monitor> {
        self.monitor.clone()
    }
}

impl MemoryPool for MonitoredMemoryPool {
    fn register(&self, _consumer: &MemoryConsumer) {
        self.inner.register(_consumer)
    }

    fn unregister(&self, _consumer: &MemoryConsumer) {
        self.inner.unregister(_consumer)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.monitor.grow(additional)
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.monitor.shrink(shrink);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.monitor.grow(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }
}

const MEMORY_RESERVATION_RECHECK_FACTOR: usize = 4;

#[derive(Debug)]
pub struct AllocationMonitoringMemoryPool {
    inner: Arc<dyn MemoryPool>,
    monitor: Arc<AllocationMonitor>,
}

impl AllocationMonitoringMemoryPool {
    pub fn new(inner: Arc<dyn MemoryPool>, monitor: Arc<AllocationMonitor>) -> Self {
        Self { inner, monitor }
    }
}

impl MemoryPool for AllocationMonitoringMemoryPool {
    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.monitor.max)
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.monitor.reserve(additional);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        // Shrinking doesn't update the monitor. This means that the
        // monitor will always see the memory reservation is increasing
        // causing the statistics to be polled from jemalloc
        // occasionally, keeping the statistics more accurate.
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> Result<()> {
        self.monitor.try_reserve(additional).map_err(|e| match e {
            AllocationMonitorError::Jemalloc { source } => {
                DataFusionError::External(Box::new(source))
            }
            AllocationMonitorError::HeapExhausted => {
                DataFusionError::ResourcesExhausted(e.to_string())
            }
        })?;
        self.inner.try_grow(reservation, additional)
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer)
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer)
    }
}

pub struct AllocationMonitor {
    max: usize,
    allocated: AtomicUsize,
    reserved: AtomicUsize,
}

impl std::fmt::Debug for AllocationMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllocationMonitor")
            .field("max", &self.max)
            .field("allocated", &self.allocated)
            .field("reserved", &self.reserved)
            .finish_non_exhaustive()
    }
}
// Allocation (Monitor --> )
// TrackingMonitor(BaseMonitor --> Does not change
impl AllocationMonitor {
    /// Create a new allocation monitor that will return an error if the
    /// amount of memory allocated exceeds `max`.
    pub fn try_new(max: usize) -> Result<Self, AllocationMonitorError> {
        let monitor = Self {
            max,
            allocated: AtomicUsize::new(0),
            reserved: AtomicUsize::new(0),
        };
        monitor.try_refresh()?;
        Ok(monitor)
    }

    /// Update the stats for the heap monitor
    fn try_refresh(&self) -> Result<(), AllocationMonitorError> {
        let reserved = self.reserved.load(Ordering::Acquire);
        self.allocated.store(
            jemalloc_stats::refresh_allocated().context(JemallocSnafu)?,
            Ordering::SeqCst,
        );
        self.reserved.fetch_sub(reserved, Ordering::Release);
        Ok(())
    }

    /// Reserve `sz` bytes of memory. This will return an error if the
    /// amount of memory allocated will exceed the maximum if the
    /// allocation were to be allowed.
    pub fn try_reserve(&self, sz: usize) -> Result<(), AllocationMonitorError> {
        let reserved = self.reserved.fetch_add(sz, Ordering::AcqRel) + sz;
        if self.allocated.load(Ordering::Acquire) + MEMORY_RESERVATION_RECHECK_FACTOR * reserved
            > self.max
        {
            // We have used more than a quarter of the memory that was considered
            // free last time we checkes the stats. Refresh the stats and check again.
            self.try_refresh()?;
            let allocated = self.allocated.load(Ordering::Acquire);
            let reserved = self.reserved.fetch_add(sz, Ordering::Acquire);
            if allocated + sz + reserved > self.max {
                return Err(AllocationMonitorError::HeapExhausted);
            }
        }

        Ok(())
    }

    /// Unconditionally reserve sz bytes of memory. This marks the
    /// additional memory as being reserved, but cannot fail. This is
    /// used to tell the monitor about memory that is being reserved
    /// outside of it's control. It will make it more likely that the
    /// next call to try_reserve will update the memory statistics.
    pub fn reserve(&self, sz: usize) {
        self.reserved.fetch_add(sz, Ordering::AcqRel);
    }
}

mod jemalloc_stats {
    use std::sync::LazyLock;
    use tikv_jemalloc_ctl::{
        Result, epoch, epoch_mib,
        stats::{allocated, allocated_mib},
    };

    static EPOCH_MIB: LazyLock<Result<epoch_mib>> = LazyLock::new(epoch::mib);
    static ALLOCATED_MIB: LazyLock<Result<allocated_mib>> = LazyLock::new(allocated::mib);

    pub(super) fn refresh_allocated() -> Result<usize> {
        (*EPOCH_MIB)?.write(0)?;
        (*ALLOCATED_MIB)?.read()
    }
}

#[derive(Debug)]
pub struct GreedyMemoryPool {
    pool_size: usize,
    used: AtomicUsize,
}

impl GreedyMemoryPool {
    /// Create a new pool that can allocate up to `pool_size` bytes
    pub fn new(pool_size: usize) -> Self {
        log_debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            used: AtomicUsize::new(0),
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let new_used = used + additional;
                (new_used <= self.pool_size).then_some(new_used)
            })
            .map_err(|used| {
                insufficient_capacity_err(
                    reservation,
                    additional,
                    self.pool_size.saturating_sub(used),
                )
            })?;
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.pool_size)
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


macro_rules! make_error {
    ($NAME_ERR:ident, $NAME_DF_ERR: ident, $ERR:ident) => { make_error!(@inner ($), $NAME_ERR, $NAME_DF_ERR, $ERR); };
    (@inner ($d:tt), $NAME_ERR:ident, $NAME_DF_ERR:ident, $ERR:ident) => {
        ::paste::paste!{
            /// Macro wraps `$ERR` to add backtrace feature
            #[macro_export]
            macro_rules! $NAME_DF_ERR {
                ($d($d args:expr),* $d(; diagnostic=$d DIAG:expr)?) => {{
                    let err =$crate::DataFusionError::$ERR(
                        ::std::format!(
                            "{}{}",
                            ::std::format!($d($d args),*),
                            $crate::DataFusionError::get_back_trace(),
                        ).into()
                    );
                    $d (
                        let err = err.with_diagnostic($d DIAG);
                    )?
                    err
                }
            }
        }

            /// Macro wraps Err(`$ERR`) to add backtrace feature
            #[macro_export]
            macro_rules! $NAME_ERR {
                ($d($d args:expr),* $d(; diagnostic = $d DIAG:expr)?) => {{
                    let err = $crate::[<_ $NAME_DF_ERR>]!($d($d args),*);
                    $d (
                        let err = err.with_diagnostic($d DIAG);
                    )?
                    Err(err)

                }}
            }


            // Note: Certain macros are used in this  crate, but not all.
            // This macro generates a use or all of them in case they are needed
            // so we allow unused code to avoid warnings when they are not used
            #[doc(hidden)]
            #[allow(unused)]
            pub use $NAME_ERR as [<_ $NAME_ERR>];
            #[doc(hidden)]
            #[allow(unused)]
            pub use $NAME_DF_ERR as [<_ $NAME_DF_ERR>];
        }
    };
}
