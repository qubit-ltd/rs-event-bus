// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Releases asynchronous facade shutdown leadership when its future is dropped.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::AsyncEventBusInner;

/// Releases shutdown leadership and wakes callers waiting for the leader.
pub(in crate::facade::async_event_bus) struct ShutdownLeaderGuard(
    /// Bus whose shutdown leader is released when this guard is dropped.
    Arc<AsyncEventBusInner>,
);

impl Drop for ShutdownLeaderGuard {
    /// Makes shutdown work available to another waiting caller.
    fn drop(&mut self) {
        self.0.shutdown_active.store(false, Ordering::Release);
        self.0.shutdown_signal.notify();
    }
}

impl ShutdownLeaderGuard {
    /// Acquires shutdown leadership cleanup for one shared bus.
    ///
    /// # Parameters
    /// - inner: bus whose active shutdown attempt is being guarded.
    ///
    /// # Returns
    /// A guard that releases leadership when dropped.
    #[must_use]
    #[inline]
    pub(in crate::facade::async_event_bus) fn new(inner: Arc<AsyncEventBusInner>) -> Self {
        Self(inner)
    }
}
