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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use qubit_id::Id;

use crate::error::SubscriptionCloseFailure;
use crate::model::SubscriberId;

/// Coordination state shared by a subscription handle and its worker.
pub(crate) struct SubscriptionControl {
    /// Bus-local object ID used in provider settlement context.
    pub(in crate::facade) id: Id,
    /// Logical subscriber identity used by provider operations.
    pub(in crate::facade) subscriber_id: SubscriberId,
    /// Cancellation flag observed by the worker loop.
    cancelled: AtomicBool,
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
    pub(in crate::facade) fn new(id: Id, subscriber_id: SubscriberId) -> Arc<Self> {
        Arc::new(Self {
            id,
            subscriber_id,
            cancelled: AtomicBool::new(false),
            worker: Mutex::new(None),
            close_error: Mutex::new(None),
            finished: Mutex::new(false),
            finished_changed: Condvar::new(),
        })
    }

    /// Publishes the worker join handle after a successful thread spawn.
    ///
    /// # Parameters
    /// - `worker`: running worker thread to join during cancellation.
    pub(in crate::facade) fn set_worker(&self, worker: JoinHandle<()>) {
        *self.worker.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(worker);
    }

    /// Returns whether cancellation was requested for this subscription.
    ///
    /// # Returns
    /// True after cancellation is requested, otherwise false.
    #[must_use = "Use the returned is cancelled."]
    #[inline]
    pub(in crate::facade) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Requests worker cancellation without waiting for an executing handler.
    pub(in crate::facade) fn request_cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Stores the provider close failure for lifecycle callers to observe.
    ///
    /// # Parameters
    /// - `error`: canonical provider close failure to retain.
    pub(in crate::facade) fn record_close_error(&self, error: Arc<SubscriptionCloseFailure>) {
        let mut slot = self
            .close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Marks receiver cleanup complete and wakes concurrent cancellers.
    pub(in crate::facade) fn mark_finished(&self) {
        *self.finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.finished_changed.notify_all();
    }

    /// Waits until the receiver worker has completed close and cleanup.
    ///
    /// This blocks only callers outside the worker's bus context.
    pub(in crate::facade) fn wait_finished(&self) {
        let mut finished = self.finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        while !*finished {
            finished = self
                .finished_changed
                .wait(finished)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}
