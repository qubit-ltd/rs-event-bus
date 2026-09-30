// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared cancellation, worker, and close-error state for one subscription.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use qubit_id::Id;

use crate::error::SubscriptionCloseFailure;
use crate::facade::internal::DeliveryMetrics;
use crate::model::SubscriberId;
use crate::model::SubscriptionStopReason;

/// Coordination state shared by a subscription handle and its worker.
pub(crate) struct SubscriptionControl {
    /// Cumulative counters retained by the public handle after receiver close.
    pub(in crate::facade) delivery_metrics: Arc<DeliveryMetrics>,
    /// Bus-local object ID used in provider settlement context.
    pub(in crate::facade) id: Id,
    /// Logical subscriber identity used by provider operations.
    pub(in crate::facade) subscriber_id: SubscriberId,
    /// Cancellation flag observed by the worker loop.
    cancelled: AtomicBool,
    /// First terminal receive cause, retained independently of close failures.
    terminal_failure: Mutex<Option<Arc<SubscriptionStopReason>>>,
    /// Linearizes new delivery work against terminal receive failure.
    start_gate: Mutex<()>,
    /// Worker thread handle retained until one caller joins it.
    pub(in crate::facade) worker: Mutex<Option<JoinHandle<()>>>,
    /// Canonical provider receiver close failure.
    pub(in crate::facade) close_error: Mutex<Option<Arc<SubscriptionCloseFailure>>>,
    /// Whether receiver close and worker cleanup have completed.
    finished: Mutex<bool>,
    /// Wakes cancellation callers waiting for worker completion.
    finished_changed: Condvar,
}

impl SubscriptionControl {
    /// Creates an active control block before its worker thread is spawned.
    ///
    /// # Parameters
    /// - `id`: bus-local subscription identity.
    /// - `subscriber_id`: logical subscriber identity.
    ///
    /// # Returns
    /// A shared control block ready to receive its worker handle.
    #[cfg(test)]
    pub(in crate::facade) fn new(id: Id, subscriber_id: SubscriberId) -> Arc<Self> {
        Self::with_metrics(id, subscriber_id, Arc::new(DeliveryMetrics::default()))
    }

    /// Creates one control with its retained counters forwarding to the bus
    /// accumulator.
    ///
    /// # Parameters
    /// - `id`: active bus-local receiver identity.
    /// - `subscriber_id`: logical subscriber name for provider diagnostics.
    /// - `delivery_metrics`: fixed cumulative counters forwarding to the bus
    ///   parent.
    ///
    /// # Returns
    /// Shared control state independent of the public handle lifetime.
    pub(in crate::facade) fn with_metrics(
        id: Id,
        subscriber_id: SubscriberId,
        delivery_metrics: Arc<DeliveryMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            delivery_metrics,
            id,
            subscriber_id,
            cancelled: AtomicBool::new(false),
            terminal_failure: Mutex::new(None),
            start_gate: Mutex::new(()),
            worker: Mutex::new(None),
            close_error: Mutex::new(None),
            finished: Mutex::new(false),
            finished_changed: Condvar::new(),
        })
    }

    /// Returns the first terminal receive cause, or None while healthy.
    ///
    /// # Returns
    /// Some canonical first cause after failure, or None while no terminal
    /// cause exists.
    pub(in crate::facade) fn terminal_failure(&self) -> Option<Arc<SubscriptionStopReason>> {
        self.terminal_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Returns whether cancellation was requested for this subscription.
    ///
    /// # Returns
    /// True after cancellation is requested, otherwise false.
    #[must_use = "observe whether cancellation was requested"]
    #[inline]
    pub(in crate::facade) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Linearizes the start of owned delivery work against a receive stop.
    /// Admitted work completes normally even if a later receive failure stops
    /// future work.
    ///
    /// # Returns
    /// `true` when the subscription has no terminal failure; `false` otherwise.
    #[must_use = "observe whether delivery work was admitted"]
    pub(in crate::facade) fn try_start(&self) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.terminal_failure().is_none()
    }

    /// Caches a receive failure once and prevents subsequent receives.
    /// Returns true only for the first cause, so callers emit one diagnostic.
    ///
    /// # Parameters
    /// - `reason`: terminal source retained only if no previous cause was
    ///   published.
    ///
    /// # Returns
    /// True only for the first cause; callers must fence scheduling before
    /// diagnostics.
    pub(in crate::facade) fn fail_receive(&self, reason: SubscriptionStopReason) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        let mut stored = self.terminal_failure.lock().unwrap_or_else(PoisonError::into_inner);
        let first = stored.is_none();
        if first {
            *stored = Some(Arc::new(reason));
        }
        self.cancelled.store(true, Ordering::Release);
        first
    }

    /// Linearizes one actual handler invocation with stop publication.
    ///
    /// # Parameters
    /// - `allowed`: checks scheduler shutdown policy while the start gate is
    ///   held.
    ///
    /// # Returns
    /// True when this invocation is admitted; the lock is released before user
    /// code.
    pub(in crate::facade) fn try_start_handler(&self, allowed: impl FnOnce(bool) -> bool) -> bool {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.terminal_failure().is_none() && allowed(self.is_cancelled())
    }

    /// Publishes the worker join handle after a successful thread spawn.
    ///
    /// # Parameters
    /// - `worker`: running worker thread to join during cancellation.
    pub(in crate::facade) fn set_worker(&self, worker: JoinHandle<()>) {
        *self.worker.lock().unwrap_or_else(PoisonError::into_inner) = Some(worker);
    }

    /// Requests worker cancellation without waiting for an executing handler.
    ///
    /// # Side Effects
    /// Publishes cancellation under the same short gate as actual handler
    /// admission. The gate is never held across user callbacks.
    pub(in crate::facade) fn request_cancel(&self) {
        let _start = self.start_gate.lock().unwrap_or_else(PoisonError::into_inner);
        self.cancelled.store(true, Ordering::Release);
    }

    /// Stores the provider close failure for lifecycle callers to observe.
    ///
    /// # Parameters
    /// - `error`: canonical provider close failure to retain.
    pub(in crate::facade) fn record_close_error(&self, error: Arc<SubscriptionCloseFailure>) {
        let mut slot = self.close_error.lock().unwrap_or_else(PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Marks receiver cleanup complete and wakes concurrent cancellers.
    pub(in crate::facade) fn mark_finished(&self) {
        *self.finished.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.finished_changed.notify_all();
    }

    /// Waits until the receiver worker has completed close and cleanup.
    ///
    /// This blocks only callers outside the worker's bus context.
    pub(in crate::facade) fn wait_finished(&self) {
        let mut finished = self.finished.lock().unwrap_or_else(PoisonError::into_inner);
        while !*finished {
            finished = self
                .finished_changed
                .wait(finished)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}
