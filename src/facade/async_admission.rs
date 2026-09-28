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

pub(super) use self::async_admission_future::AsyncAdmissionFuture;
pub(super) use self::async_admission_permit::AsyncAdmissionPermit;
use self::async_admission_state::AsyncAdmissionState;

mod async_admission_future;
mod async_admission_permit;
mod async_admission_state;

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
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(AsyncAdmissionState::default()),
            next_waiter: AtomicU64::new(1),
        })
    }

    /// Returns a future that waits for the next bus-wide delivery slot.
    pub(super) fn acquire(self: &Arc<Self>) -> AsyncAdmissionFuture {
        AsyncAdmissionFuture::new(
            self.clone(),
            self.next_waiter.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        )
    }
}
