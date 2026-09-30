// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII completion accounting for asynchronous subscribe calls.

use std::sync::Arc;

use super::AsyncTracker;

/// Releases one already-counted subscribe operation when the call completes.
#[must_use]
pub(in crate::facade) struct AsyncSubscribeGuard(
    /// Tracker whose active subscribe count this guard owns.
    Arc<AsyncTracker>,
);

impl AsyncSubscribeGuard {
    /// Wraps a subscribe already incremented by the operation admission path.
    ///
    /// # Parameters
    /// - `tracker`: tracker with one active subscribe operation already
    ///   counted.
    ///
    /// # Returns
    /// A guard that releases the subscribe count when dropped.
    #[inline]
    pub(in crate::facade) fn after_start(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncSubscribeGuard {
    /// Releases the counted subscribe operation.
    #[inline]
    fn drop(&mut self) {
        self.0.subscribe_finished();
    }
}
