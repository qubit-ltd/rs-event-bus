// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Final owner bookkeeping after receiver and delivery cleanup.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::PoisonError;

use crate::facade::SubscriptionControl;
use crate::facade::event_bus::EventBusInner;
use crate::facade::internal::LifecycleState;

/// Completes tracking and cancellation waiters even if registration cleanup
/// panics.
#[must_use = "dropping this guard ends owner lifecycle tracking"]
pub(in crate::facade) struct OwnerLifecycleGuard {
    /// Shared bus state retained through completion.
    inner: Arc<EventBusInner>,
    /// Owner's completion notification and first failure state.
    control: Arc<SubscriptionControl>,
}

impl OwnerLifecycleGuard {
    /// Installs lifecycle cleanup before owner work begins.
    ///
    /// # Parameters
    /// - `inner`: bus whose registry and tracker contain this worker.
    /// - `control`: subscription to mark finished after cleanup.
    ///
    /// # Returns
    /// A guard dropped after the owner's delivery map.
    #[inline]
    pub(in crate::facade) fn new(inner: Arc<EventBusInner>, control: Arc<SubscriptionControl>) -> Self {
        Self { inner, control }
    }
}

impl Drop for OwnerLifecycleGuard {
    /// Releases scheduler, tracker, registry, and lifecycle ownership at exit.
    ///
    /// Scheduler cleanup panics are contained so the remaining owner
    /// bookkeeping still runs.
    fn drop(&mut self) {
        let cleanup = catch_unwind(AssertUnwindSafe(|| {
            self.inner.scheduler.finish_subscription(self.control.id);
        }));
        self.inner.tracker.worker_finished();
        self.inner
            .subscriptions
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&self.control.id);
        let mut lifecycle = self.inner.lifecycle.lock().unwrap_or_else(PoisonError::into_inner);
        if *lifecycle == LifecycleState::Closing
            && self.inner.tracker.workers_are_idle()
            && self
                .inner
                .shutdown_gate
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .report
                .is_some()
        {
            *lifecycle = LifecycleState::Closed;
        }
        drop(lifecycle);
        self.control.mark_finished();
        if cleanup.is_err() {
            self.inner
                .emit_internal("owner_cleanup", "scheduler registration cleanup panicked".into());
        }
    }
}
