// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publishes worker completion even when processing or cleanup unwinds.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::thread::panicking;

use super::worker_exit::WorkerExit;
use super::worker_state::WorkerState;
use crate::notification::notification_stats::NotificationStats;

/// Publishes the worker's terminal state when its processing scope ends.
///
/// The guard owns shared references to the completion state and statistics, but
/// no worker resources. It defaults to `Panicked`; callers mark it drained only
/// after processing and cleanup succeed. On drop, an active unwind takes
/// precedence over that mark. The guard records the outcome once, recovers a
/// poisoned state lock, and wakes all waiting closers.
#[must_use = "retain the guard until worker processing and cleanup have completed"]
pub(in crate::notification) struct WorkerCompletionGuard {
    /// State and wakeup shared with every closer.
    state: Arc<(Mutex<WorkerState>, Condvar)>,
    /// Counters updated before terminal state becomes observable.
    stats: Arc<NotificationStats>,
    /// Defaults to failure until owned-resource cleanup succeeds.
    exit: WorkerExit,
}

impl WorkerCompletionGuard {
    /// Arms completion before processing resources enter the outer unwind
    /// boundary.
    ///
    /// `state` and `stats` are internal shared state, without user callbacks.
    ///
    /// # Parameters
    /// - `state`: completion state and condition variable shared with closers.
    /// - `stats`: counters updated before completion becomes observable.
    ///
    /// # Returns
    /// A guard that publishes `Panicked` unless processing and cleanup drain.
    ///
    /// The supplied `Arc`s are retained until the guard is dropped; no user
    /// callback is invoked while publishing completion.
    #[inline]
    pub(in crate::notification) fn new(
        state: Arc<(Mutex<WorkerState>, Condvar)>,
        stats: Arc<NotificationStats>,
    ) -> Self {
        Self {
            state,
            stats,
            exit: WorkerExit::Panicked,
        }
    }

    /// Records successful processing and cleanup before publishing completion.
    ///
    /// The mark is used when the guard is later dropped outside an unwind. If
    /// the thread is unwinding at drop time, the published outcome remains
    /// `Panicked`.
    #[inline]
    pub(in crate::notification) fn mark_drained(&mut self) {
        self.exit = WorkerExit::Drained;
    }
}

impl Drop for WorkerCompletionGuard {
    /// Publishes one immutable result and wakes all closers without user code.
    fn drop(&mut self) {
        let exit = if panicking() { WorkerExit::Panicked } else { self.exit };
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(PoisonError::into_inner);
        if state.exit.is_none() {
            if exit == WorkerExit::Panicked {
                NotificationStats::increment(&self.stats.worker_panicked);
            }
            state.exit = Some(exit);
        }
        drop(state);
        changed.notify_all();
    }
}
