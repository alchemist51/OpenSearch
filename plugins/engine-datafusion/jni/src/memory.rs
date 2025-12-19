use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use datafusion::common::DataFusionError;
use snafu::{ResultExt, Snafu};

pub type Result<T, E = DataFusionError> = result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum AllocationMonitorError {
    #[snafu(display("Error getting jemalloc stats: {}", source))]
    Jemalloc { source: tikv_jemalloc_ctl::Error },

    #[snafu(display("Heap exhausted"))]
    HeapExhausted,
}

const MEMORY_RESERVATION_RECHECK_FACTOR: usize = 4;

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
        self.allocated.store(
            jemalloc_stats::refresh_allocated().context(JemallocSnafu)?,
            Ordering::SeqCst,
        );
        self.reserved.store(0, Ordering::Release);
        Ok(())
    }

    /// Reserve `sz` bytes of memory. This will return an error if the
    /// amount of memory allocated will exceed the maximum if the
    /// allocation were to be allowed.
    pub fn try_reserve(&self, sz: usize) -> Result<()> {
        let reserved = self.reserved.fetch_add(sz, Ordering::AcqRel);
        let allocated = self.allocated.load(Ordering::Acquire);

        if allocated + MEMORY_RESERVATION_RECHECK_FACTOR * (reserved + sz) > self.max {
            log_info!("Refreshing stats. sz: {}, reserved: {}, max: {}", sz, reserved + sz, self.max);

            self.try_refresh().map_err(|_| {
                DataFusionError::ResourcesExhausted("Failed to allocate additional memory".to_string())
            })?;

            let allocated = self.allocated.load(Ordering::Acquire);
            let reserved = self.reserved.load(Ordering::Acquire);

            log_info!("New Allocated stats: {}, sz: {}, reserved: {}, max: {}", allocated, sz, reserved, self.max);

            if allocated + sz + reserved > self.max {
                log_info!("Heap exhausted. Allocated: {}, sz: {}, reserved: {}, max: {}", allocated, sz, reserved, self.max);
                return Err(DataFusionError::ResourcesExhausted(format!(
                    "Failed to allocate additional {} bytes with {} bytes already allocated - {} bytes remain available for the total pool",
                    sz, allocated + reserved, self.max.saturating_sub(allocated + reserved)
                )));
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
    monitor: Arc<AllocationMonitor>
}

impl GreedyMemoryPool {
    /// Create a new pool that can allocate up to `pool_size` bytes
    pub fn new(pool_size: usize) -> Self {
        log_debug!("Created new GreedyMemoryPool(pool_size={pool_size})");
        Self {
            pool_size,
            used: AtomicUsize::new(0),
            monitor: Arc::new(AllocationMonitor::try_new(pool_size).unwrap())
        }
    }
}

impl MemoryPool for GreedyMemoryPool {
    fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
        self.monitor.reserve(additional);
        self.used.fetch_add(additional, Ordering::Relaxed);
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        self.used.fetch_sub(shrink, Ordering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.monitor.try_reserve(additional).map_err(|e| match e {
            _ => {
                insufficient_capacity_err(reservation, additional, self.pool_size)
            }
        })?;

        let allocated = self.monitor.allocated.load(Ordering::Acquire);
        let reserved = self.monitor.reserved.load(Ordering::Acquire);
        log_info!("allocated:{:?}, reserved: {:?}", allocated, reserved);


        self.used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                let used = allocated + reserved;
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
