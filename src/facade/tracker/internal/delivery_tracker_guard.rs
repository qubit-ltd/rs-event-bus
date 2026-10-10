// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop-based release of one topic's in-flight delivery count.

use super::super::LifecycleTracker;

/// Releases one topic's in-flight count on every worker exit path.
#[must_use = "dropping this guard releases the tracked delivery count"]
pub(crate) struct DeliveryTrackerGuard<'a> {
    /// Tracker whose topic count this guard owns.
    tracker: &'a LifecycleTracker,
    /// Topic bucket decremented when this guard is dropped.
    topic: Box<str>,
}

impl<'a> DeliveryTrackerGuard<'a> {
    /// Creates a guard for one topic delivery already recorded by the tracker.
    ///
    /// # Parameters
    /// - `tracker`: tracker whose count this guard releases.
    /// - `topic`: topic bucket incremented before guard creation.
    ///
    /// # Returns
    /// A guard that decrements the topic count on drop.
    #[inline]
    pub(in crate::facade) fn new(tracker: &'a LifecycleTracker, topic: Box<str>) -> Self {
        Self { tracker, topic }
    }
}

impl Drop for DeliveryTrackerGuard<'_> {
    /// Decrements the topic count and wakes idle/shutdown waiters.
    fn drop(&mut self) {
        self.tracker.finish_delivery(&self.topic);
    }
}
