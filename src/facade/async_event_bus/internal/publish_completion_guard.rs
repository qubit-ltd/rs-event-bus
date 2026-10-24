// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tracks whether an asynchronous publish completed or was cancelled.

use crate::facade::PublishMetrics;

/// Counts a polled publish as cancelled if it is dropped before returning.
pub(in crate::facade) struct PublishCompletionGuard<'a> {
    /// Shared counters updated when the pending publish is cancelled.
    metrics: &'a PublishMetrics,
    /// Whether the publish returned and should not count as cancelled.
    completed: bool,
}

impl<'a> PublishCompletionGuard<'a> {
    /// Starts tracking an in-flight asynchronous publish.
    #[inline]
    pub(in crate::facade) fn new(metrics: &'a PublishMetrics) -> Self {
        Self {
            metrics,
            completed: false,
        }
    }

    /// Marks the publish as returned so dropping the guard does not count it.
    #[inline]
    pub(in crate::facade) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for PublishCompletionGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.record_cancelled();
        }
    }
}
