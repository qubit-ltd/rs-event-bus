// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registration guard that removes its waker when dropped.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Waker;

/// Guard that unregisters one task waker when its wait is canceled or ends.
#[must_use]
pub(in crate::local) struct AsyncWaiter {
    /// Registration ID removed when this guard is dropped.
    id: u64,
    /// Waker registry containing this waiter's registration.
    waiters: Arc<Mutex<HashMap<u64, Waker>>>,
}

impl AsyncWaiter {
    /// Creates a guard for a previously inserted waker.
    ///
    /// # Parameters
    /// - `id`: waiter registration ID.
    /// - `waiters`: shared registry containing the registration.
    ///
    /// # Returns
    /// A guard that removes the registration on drop.
    pub(in crate::local) fn new(id: u64, waiters: Arc<Mutex<HashMap<u64, Waker>>>) -> Self {
        Self { id, waiters }
    }
}

impl Drop for AsyncWaiter {
    /// Removes this registration from the shared waiter registry.
    fn drop(&mut self) {
        let removed = {
            let mut waiters = self.waiters.lock().unwrap_or_else(PoisonError::into_inner);
            waiters.remove(&self.id)
        };
        drop(removed);
    }
}
