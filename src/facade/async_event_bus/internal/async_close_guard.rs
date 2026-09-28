// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII completion accounting for asynchronous subscription close calls.

use std::sync::Arc;

use super::AsyncTracker;

/// Releases one counted close operation when its caller exits or is cancelled.
pub(in crate::facade) struct AsyncCloseGuard(Arc<AsyncTracker>);

impl AsyncCloseGuard {
    /// Associates the guard with a close operation counted by the tracker.
    pub(in crate::facade) fn new(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncCloseGuard {
    fn drop(&mut self) {
        self.0.close_finished();
    }
}
