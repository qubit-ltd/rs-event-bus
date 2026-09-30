// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII completion accounting for asynchronous publish calls.

use std::sync::Arc;

use super::AsyncTracker;

/// Releases one already-counted publish operation when the call completes.
#[must_use = "the publish operation guard must remain alive until completion"]
pub(in crate::facade) struct AsyncPublishGuard(
    /// Tracker whose active publish count this guard owns.
    Arc<AsyncTracker>,
);

impl AsyncPublishGuard {
    /// Wraps a publish already incremented by the operation admission path.
    ///
    /// # Parameters
    /// - `tracker`: tracker with one active publish operation already counted.
    ///
    /// # Returns
    /// A guard that releases the publish count when dropped.
    #[inline]
    pub(in crate::facade) fn after_start(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncPublishGuard {
    /// Releases the counted publish operation.
    fn drop(&mut self) {
        self.0.publish_finished();
    }
}
