// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async facade lifecycle owner.

use crate::facade::async_event_bus::Arc;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::async_event_bus::Ordering;

pub(in crate::facade) struct ShutdownLeaderGuard(
    /// Shared bus state whose shutdown leadership is being released.
    pub(in crate::facade) Arc<AsyncEventBusInner>,
);
impl Drop for ShutdownLeaderGuard {
    /// Makes shutdown work available to another waiting caller.
    fn drop(&mut self) {
        self.0.shutdown_active.store(false, Ordering::Release);
        self.0.shutdown_signal.notify();
    }
}
