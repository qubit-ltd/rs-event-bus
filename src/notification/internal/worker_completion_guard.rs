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
use std::thread;

use super::worker_exit::WorkerExit;
use super::worker_state::WorkerState;
use crate::notification::notification_stats::NotificationStats;

/// Sole publisher of the worker's terminal state; owns no user resources.
#[must_use = "the completion guard must stay alive until processing ends"]
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
        let exit = if thread::panicking() {
            WorkerExit::Panicked
        } else {
            self.exit
        };
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

#[cfg(test)]
mod tests {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;

    use super::WorkerCompletionGuard;
    use crate::notification::internal::worker_exit::WorkerExit;
    use crate::notification::internal::worker_state::WorkerState;
    use crate::notification::notification_stats::NotificationStats;

    fn completion_state() -> Arc<(Mutex<WorkerState>, Condvar)> {
        Arc::new((Mutex::new(WorkerState { exit: None }), Condvar::new()))
    }

    fn terminal_exit(state: &Arc<(Mutex<WorkerState>, Condvar)>) -> WorkerExit {
        let (lock, _) = &**state;
        lock.lock()
            .expect("worker state lock is not poisoned")
            .exit
            .expect("guard publishes a terminal result")
    }

    #[test]
    fn test_drop_without_drain_publishes_panicked_and_increments_counter() {
        let state = completion_state();
        let stats = Arc::new(NotificationStats::default());

        drop(WorkerCompletionGuard::new(Arc::clone(&state), Arc::clone(&stats)));

        assert_eq!(WorkerExit::Panicked, terminal_exit(&state));
        assert_eq!(1, stats.snapshot().worker_panicked());
    }

    #[test]
    fn test_mark_drained_publishes_success_without_panic_counter() {
        let state = completion_state();
        let stats = Arc::new(NotificationStats::default());
        let mut guard = WorkerCompletionGuard::new(Arc::clone(&state), Arc::clone(&stats));
        guard.mark_drained();

        drop(guard);

        assert_eq!(WorkerExit::Drained, terminal_exit(&state));
        assert_eq!(0, stats.snapshot().worker_panicked());
    }

    #[test]
    fn test_unwinding_overrides_marked_drained_result() {
        let state = completion_state();
        let stats = Arc::new(NotificationStats::default());

        let result = catch_unwind(AssertUnwindSafe(|| {
            let mut guard = WorkerCompletionGuard::new(Arc::clone(&state), Arc::clone(&stats));
            guard.mark_drained();
            panic!("worker processing unwinds before completion");
        }));

        assert!(result.is_err());
        assert_eq!(WorkerExit::Panicked, terminal_exit(&state));
        assert_eq!(1, stats.snapshot().worker_panicked());
    }
}
