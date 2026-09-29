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

use super::worker_exit::WorkerExit;
use super::worker_state::WorkerState;
use crate::notification::notification_stats::NotificationStats;

/// Sole publisher of the worker's terminal state; owns no user resources.
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
    pub(in crate::notification) fn mark_drained(&mut self) {
        self.exit = WorkerExit::Drained;
    }
}

impl Drop for WorkerCompletionGuard {
    /// Publishes one immutable result and wakes all closers without user code.
    fn drop(&mut self) {
        let exit = if std::thread::panicking() {
            WorkerExit::Panicked
        } else {
            self.exit
        };
        let (lock, changed) = &*self.state;
        let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
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
