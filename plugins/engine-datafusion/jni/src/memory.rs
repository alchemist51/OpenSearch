// /*
//  * SPDX-License-Identifier: Apache-2.0
//  *
//  * The OpenSearch Contributors require contributions made to
//  * this file be licensed under the Apache-2.0 license or a
//  * compatible open source license.
//  */
// use std::num::NonZeroUsize;
// use std::sync::Arc;
// use std::sync::atomic::AtomicUsize;
// use datafusion::execution::context;
// use datafusion::execution::memory_pool::{MemoryPool, MemoryReservation};
//
// #[derive(Debug)]
// pub struct PerQueryMemoryPool {
//     /// The [`MemoryReservation`] for the central memory pool.
//     /// This is used when the allocated memory exceeds the pre-reserved limit.
//     central_reservation: Arc<std::sync::Mutex<MemoryReservation>>,
//
//     /// The current allocated size for this query. If this value exceeds
//     /// the `pre_reserved` value, the excess amount is allocated from the
//     /// central memory pool.
//     allocated: AtomicUsize,
//
//     /// The pre-reserved size for this query in the [`PerQueryMemoryPool`].
//     /// This is a fixed value and represents the initial memory allocation
//     /// for each query.
//     pre_reserved: NonZeroUsize,
// }
//
// impl PerQueryMemoryPool {
//     pub fn new(reservation: MemoryReservation, pre_reserved: NonZeroUsize) -> Self {
//         Self {
//             central_reservation: Arc::new(std::sync::Mutex::new(reservation)),
//             allocated: AtomicUsize::new(0),
//             pre_reserved,
//         }
//     }
//
//     /// Resize the central memory reservation to the currently allocated amount.
//     ///
//     /// It does not matter whether a thread attempts to grow or shrink the allocation;
//     /// as long as the central memory reservation reflects the current memory requirement
//     /// at a given point in time, the behavior is correct. This function can be a no-op
//     /// if another thread has already updated the central memory reservation.
//     fn sync_central_reservation(&self) {
//         let mut reservation = self
//             .central_reservation
//             .lock()
//             .expect("Acquired central reservation");
//
//         // Load the allocated value again to avoid race condition
//         let allocated_load = self.allocated.load(std::sync::atomic::Ordering::SeqCst);
//
//         // Use `saturating_sub` to avoid underflow
//         reservation.resize(allocated_load.saturating_sub(self.pre_reserved.into()));
//     }
//
//     /// Try to resize the central memory reservation to the currently allocated amount.
//     ///
//     /// It does not matter whether a thread attempts to grow or shrink the allocation;
//     /// as long as the central memory reservation reflects the current memory requirement
//     /// at a given point in time, the behavior is correct. This function can be a no-op
//     /// if another thread has already updated the central memory reservation.
//     ///
//     /// # Errors
//     /// An error may occur if the lock fails to be acquired, or if resizing the central memory pool
//     /// fails. In either case, the central memory reservation is not updated. The caller of this
//     /// function may need to revert the allocation in the per-query memory pool.
//     fn try_sync_central_reservation(&self) -> context::Result<()> {
//         if let Ok(mut reservation) = self.central_reservation.lock() {
//             // Successfully acquire the lock for the "central memory pool"
//
//             // Load the allocated value again to avoid race condition.
//             let allocated_load = self.allocated.load(std::sync::atomic::Ordering::SeqCst);
//
//             // Use `saturating_sub` to prevent underflow
//             reservation.try_resize(allocated_load.saturating_sub(self.pre_reserved.into()))
//         } else {
//             // Lock acquisition may fail if another thread panicked while holding the mutex.
//             // Since we are unable to allocate additional memory for this query, return an error.
//             Err(datafusion::error::DataFusionError::ResourcesExhausted(
//                 String::from("Cannot allocate more memory for this query in PerQueryMemoryPool"),
//             ))
//         }
//     }
// }
//
// impl MemoryPool for PerQueryMemoryPool {
//     fn grow(&self, _reservation: &MemoryReservation, additional: usize) {
//         let old = self
//             .allocated
//             .fetch_add(additional, std::sync::atomic::Ordering::SeqCst);
//         let allocated = old + additional;
//
//         if allocated > self.pre_reserved.into() {
//             self.sync_central_reservation();
//         }
//     }
//
//     fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
//         if shrink > self.allocated.load(std::sync::atomic::Ordering::SeqCst) {
//             // Do not shrink to avoid underflowing
//             return;
//         }
//
//         let old = self
//             .allocated
//             .fetch_sub(shrink, std::sync::atomic::Ordering::SeqCst);
//
//         if old > self.pre_reserved.into() {
//             self.sync_central_reservation();
//         }
//     }
//
//     fn try_grow(
//         &self,
//         _reservation: &MemoryReservation,
//         additional: usize,
//     ) -> context::Result<()> {
//         let old = self
//             .allocated
//             .fetch_add(additional, std::sync::atomic::Ordering::SeqCst);
//         let allocated = old + additional;
//
//         let pre_reserved = self.pre_reserved.into();
//
//         if allocated <= pre_reserved {
//             return Ok(());
//         }
//
//         self.try_sync_central_reservation().inspect_err(|_| {
//             // Failed to sync the central memory pool reservation, so revert the
//             // allocation in the per-query memory pool
//             self.allocated
//                 .fetch_sub(additional, std::sync::atomic::Ordering::SeqCst);
//         })
//     }
//
//     fn reserved(&self) -> usize {
//         self.allocated.load(std::sync::atomic::Ordering::Relaxed)
//     }
// }
