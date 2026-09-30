// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fair bus-wide admission for asynchronous deliveries.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

pub(super) use self::internal::AsyncAdmissionFuture;
pub(super) use self::internal::AsyncAdmissionPermit;
use self::internal::AsyncAdmissionState;

mod internal;

/// Bus-wide asynchronous delivery admission with cancellation-safe FIFO
/// waiters.
pub(super) struct AsyncAdmission {
    /// Maximum number of deliveries admitted across all subscriptions.
    pub(super) limit: usize,
    /// In-flight permits and waiter queue protected as one state snapshot.
    pub(super) state: Mutex<AsyncAdmissionState>,
    /// Generates unique IDs for cancellable queue entries.
    next_waiter: AtomicU64,
}

impl AsyncAdmission {
    /// Creates a shared admission gate with the configured in-flight limit.
    ///
    /// # Parameters
    /// - `limit`: maximum number of active delivery permits.
    ///
    /// # Returns
    /// A shared gate with an empty waiter queue and zero in-flight permits.
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(AsyncAdmissionState::default()),
            next_waiter: AtomicU64::new(1),
        })
    }

    /// Returns a future that waits for the next bus-wide delivery slot.
    ///
    /// # Returns
    /// A future that joins the gate's FIFO queue when first polled.
    #[inline]
    pub(super) fn acquire(self: &Arc<Self>) -> AsyncAdmissionFuture {
        AsyncAdmissionFuture::new(self.clone(), self.next_waiter.fetch_add(1, Ordering::Relaxed))
    }
}
