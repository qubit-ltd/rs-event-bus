// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII completion accounting for asynchronous subscription runners.

use std::sync::Arc;

use super::AsyncTracker;

/// Decrements the active runner count when a runner future is dropped.
#[must_use]
pub(in crate::facade) struct AsyncRunnerGuard(
    /// Tracker whose active runner count this guard owns.
    Arc<AsyncTracker>,
);

impl AsyncRunnerGuard {
    /// Increments the tracker and creates a guard for one active runner.
    ///
    /// # Parameters
    /// - `tracker`: lifecycle tracker whose active-runner count is incremented.
    ///
    /// # Returns
    /// A guard that releases the runner count when dropped.
    pub(in crate::facade) fn enter(tracker: Arc<AsyncTracker>) -> Self {
        tracker.runner_started();
        Self(tracker)
    }
}

impl Drop for AsyncRunnerGuard {
    /// Releases the runner count.
    fn drop(&mut self) {
        self.0.runner_finished();
    }
}
