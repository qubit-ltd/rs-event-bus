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
pub(in crate::facade) struct AsyncRunnerGuard(Arc<AsyncTracker>);

impl AsyncRunnerGuard {
    /// Increments the tracker and creates a guard for one active runner.
    pub(in crate::facade) fn enter(tracker: Arc<AsyncTracker>) -> Self {
        tracker.runner_started();
        Self(tracker)
    }
}

impl Drop for AsyncRunnerGuard {
    fn drop(&mut self) {
        self.0.runner_finished();
    }
}
