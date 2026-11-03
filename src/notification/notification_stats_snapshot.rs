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
/// consistent view. `published` counts successful receipts, including dropped
/// receipts, not completed handlers. Admission counters classify those
/// receipts; opaque acceptance does not reveal a destination count. Each
/// counter saturates at `u64::MAX`, so sums lose exactness after saturation.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::NotificationStatsSnapshot;
///
/// let snapshot = NotificationStatsSnapshot::default();
/// assert_eq!(snapshot.enqueued(), 0);
/// assert_eq!(snapshot.worker_panicked(), 0);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NotificationStatsSnapshot {
    /// Number of notification payloads accepted into the queue.
    pub(super) enqueued: u64,
    /// Number of attempts rejected because the queue was full.
    pub(super) queue_full: u64,
    /// Number of attempts rejected because the publisher was closed.
    pub(super) queue_closed: u64,
    /// Number of provider admission receipts returned to the worker.
    pub(super) published: u64,
    /// Number of facade or provider publication errors.
    pub(super) publish_errors: u64,
    /// Number of notification request construction errors.
    pub(super) request_errors: u64,
    /// Number of observer panics contained by the worker.
    pub(super) observer_panicked: u64,
    /// Number of worker panics outside observer callbacks.
    pub(super) worker_panicked: u64,
    /// Number of receipts reporting complete destination acceptance.
    pub(super) admission_accepted: u64,
    /// Number of receipts reporting partial destination acceptance.
    pub(super) admission_partially_accepted: u64,
    /// Number of receipts reporting no accepted destinations.
    pub(super) admission_none_accepted: u64,
    /// Number of receipts reporting no destinations.
    pub(super) admission_no_destinations: u64,
    /// Number of opaque provider acceptance receipts.
    pub(super) admission_opaque_accepted: u64,
    /// Number of receipts reporting an interceptor drop.
    pub(super) admission_dropped: u64,
}

impl NotificationStatsSnapshot {
    /// Returns the number of notifications accepted into the local queue.
    ///
    /// # Returns
    /// The number of notification payloads accepted into the queue.
    #[must_use]
    #[inline]
    pub const fn enqueued(self) -> u64 {
        self.enqueued
    }

    /// Returns the number of attempts rejected because the queue was full.
    ///
    /// # Returns
    /// The number of attempts rejected because the queue was full.
    #[must_use]
    #[inline]
    pub const fn queue_full(self) -> u64 {
        self.queue_full
    }

    /// Returns the number of attempts rejected after the publisher closed.
    ///
    /// # Returns
    /// The number of attempts rejected after the publisher closed.
    #[must_use]
    #[inline]
    pub const fn queue_closed(self) -> u64 {
        self.queue_closed
    }

    /// Returns the number of provider admission receipts observed.
    ///
    /// # Returns
    /// The number of provider admission receipts returned to the worker.
    #[must_use]
    #[inline]
    pub const fn published(self) -> u64 {
        self.published
    }

    /// Returns the number of facade or provider publication errors observed.
    ///
    /// # Returns
    /// The number of facade or provider publication errors.
    #[must_use]
    #[inline]
    pub const fn publish_errors(self) -> u64 {
        self.publish_errors
    }

    /// Returns the number of notification request construction errors observed.
    ///
    /// # Returns
    /// The number of notification request construction errors.
    #[must_use]
    #[inline]
    pub const fn request_errors(self) -> u64 {
        self.request_errors
    }

    /// Returns the number of observer panics contained by the worker.
    ///
    /// # Returns
    /// The number of observer panics caught by the worker.
    #[must_use]
    #[inline]
    pub const fn observer_panicked(self) -> u64 {
        self.observer_panicked
    }

    /// Returns the number of notification worker panics outside observers.
    ///
    /// # Returns
    /// The number of worker panics outside observer callbacks.
    #[must_use]
    #[inline]
    pub const fn worker_panicked(self) -> u64 {
        self.worker_panicked
    }

    /// Returns receipts reporting complete destination acceptance.
    #[must_use]
    #[inline]
    pub const fn admission_accepted(self) -> u64 {
        self.admission_accepted
    }

    /// Returns receipts reporting partial destination acceptance.
    #[must_use]
    #[inline]
    pub const fn admission_partially_accepted(self) -> u64 {
        self.admission_partially_accepted
    }

    /// Returns receipts reporting no accepted destinations.
    #[must_use]
    #[inline]
    pub const fn admission_none_accepted(self) -> u64 {
        self.admission_none_accepted
    }

    /// Returns receipts reporting no destinations.
    #[must_use]
    #[inline]
    pub const fn admission_no_destinations(self) -> u64 {
        self.admission_no_destinations
    }

    /// Returns opaque provider acceptance receipts.
    #[must_use]
    #[inline]
    pub const fn admission_opaque_accepted(self) -> u64 {
        self.admission_opaque_accepted
    }

    /// Returns receipts reporting an interceptor drop.
    #[must_use]
    #[inline]
    pub const fn admission_dropped(self) -> u64 {
        self.admission_dropped
    }
}
