// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public snapshot of notification worker counters.

/// A concurrent snapshot of the local notification worker counters.
///
/// Counters are loaded independently, so a snapshot is not a transactionally
/// consistent view. `published` counts receipts, not completed handlers.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NotificationStatsSnapshot {
    pub(super) enqueued: u64,
    pub(super) queue_full: u64,
    pub(super) queue_closed: u64,
    pub(super) published: u64,
    pub(super) publish_errors: u64,
    pub(super) request_errors: u64,
    pub(super) observer_panicked: u64,
    pub(super) worker_panicked: u64,
}

impl NotificationStatsSnapshot {
    /// Returns the number of notifications accepted into the local queue.
    #[must_use]
    pub const fn enqueued(self) -> u64 {
        self.enqueued
    }

    /// Returns the number of attempts rejected because the queue was full.
    #[must_use]
    pub const fn queue_full(self) -> u64 {
        self.queue_full
    }

    /// Returns the number of attempts rejected after the publisher closed.
    #[must_use]
    pub const fn queue_closed(self) -> u64 {
        self.queue_closed
    }

    /// Returns the number of provider admission receipts observed.
    #[must_use]
    pub const fn published(self) -> u64 {
        self.published
    }

    /// Returns the number of facade or provider publication errors observed.
    #[must_use]
    pub const fn publish_errors(self) -> u64 {
        self.publish_errors
    }

    /// Returns the number of event request construction errors observed.
    #[must_use]
    pub const fn request_errors(self) -> u64 {
        self.request_errors
    }

    /// Returns the number of observer panics contained by the worker.
    #[must_use]
    pub const fn observer_panicked(self) -> u64 {
        self.observer_panicked
    }

    /// Returns whether the notification worker panicked outside an observer.
    #[must_use]
    pub const fn worker_panicked(self) -> u64 {
        self.worker_panicked
    }
}
