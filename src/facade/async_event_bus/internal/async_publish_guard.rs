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
pub(in crate::facade) struct AsyncPublishGuard(Arc<AsyncTracker>);

impl AsyncPublishGuard {
    /// Wraps a publish already incremented by the operation admission path.
    pub(in crate::facade) fn after_start(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncPublishGuard {
    fn drop(&mut self) {
        self.0.publish_finished();
    }
}
