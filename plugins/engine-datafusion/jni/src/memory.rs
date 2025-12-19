/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
use std::result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use std::sync::Arc;
use datafusion::common::DataFusionError;
use crate::jemalloc_monitor::{AllocationMonitor, AllocationMonitorError};

pub type Result<T, E = DataFusionError> = result::Result<T, E>;


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
