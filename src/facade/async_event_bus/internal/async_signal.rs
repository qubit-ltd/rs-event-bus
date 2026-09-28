// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Waker registration for asynchronous facade state changes.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Wakes tasks waiting for asynchronous facade state changes.
#[derive(Default)]
pub(in crate::facade) struct AsyncSignal {
    wakers: Mutex<HashMap<u64, std::task::Waker>>,
    next_waiter: AtomicU64,
}

impl AsyncSignal {
    /// Wakes all registered waiters after releasing the registry lock.
    pub(in crate::facade) fn notify(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for (_, waker) in wakers {
            waker.wake();
        }
    }

    /// Registers or replaces a waiter's current waker.
    pub(in crate::facade) fn register_waiter(&self, id: u64, waker: &std::task::Waker) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, waker.clone());
    }

    /// Removes a waiter's registration when its future completes or is dropped.
    pub(in crate::facade) fn unregister(&self, id: u64) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    /// Allocates a unique identifier for a new wait registration.
    pub(in crate::facade) fn next_waiter_id(&self) -> u64 {
        self.next_waiter.fetch_add(1, Ordering::Relaxed)
    }
}
