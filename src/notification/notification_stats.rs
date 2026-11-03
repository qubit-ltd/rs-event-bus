// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal counters shared by the notification publisher and its worker.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::notification_stats_snapshot::NotificationStatsSnapshot;
use crate::model::AdmissionOutcome;

/// Atomic counters shared by the publisher handle and its worker.
#[derive(Default)]
pub(super) struct NotificationStats {
    /// Number of notifications accepted into the queue.
    pub(super) enqueued: AtomicU64,
    /// Number of notifications rejected because the queue was full.
    pub(super) queue_full: AtomicU64,
    /// Number of notifications rejected because the queue was closed.
    pub(super) queue_closed: AtomicU64,
    /// Number of notifications published by the worker.
    pub(super) published: AtomicU64,
    /// Number of provider publication failures.
    pub(super) publish_errors: AtomicU64,
    /// Number of invalid notification requests.
    pub(super) request_errors: AtomicU64,
    /// Number of observer callbacks that panicked.
    pub(super) observer_panicked: AtomicU64,
    /// Number of worker failures caused by a panic.
    pub(super) worker_panicked: AtomicU64,
    /// Number of receipts reporting complete destination acceptance.
    pub(super) admission_accepted: AtomicU64,
    /// Number of receipts reporting partial destination acceptance.
    pub(super) admission_partially_accepted: AtomicU64,
    /// Number of receipts reporting no accepted destinations.
    pub(super) admission_none_accepted: AtomicU64,
    /// Number of receipts reporting no destinations.
    pub(super) admission_no_destinations: AtomicU64,
    /// Number of opaque provider acceptance receipts.
    pub(super) admission_opaque_accepted: AtomicU64,
    /// Number of receipts reporting an interceptor drop.
    pub(super) admission_dropped: AtomicU64,
}

impl NotificationStats {
    /// Loads all counters into one best-effort snapshot.
    ///
    /// # Returns
    /// A snapshot containing relaxed atomic loads of the current counters. The
    /// values are not guaranteed to represent one globally consistent instant.
    #[must_use = "Use the returned query result."]
    pub(super) fn snapshot(&self) -> NotificationStatsSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        NotificationStatsSnapshot {
            enqueued: load(&self.enqueued),
            queue_full: load(&self.queue_full),
            queue_closed: load(&self.queue_closed),
            published: load(&self.published),
            publish_errors: load(&self.publish_errors),
            request_errors: load(&self.request_errors),
            observer_panicked: load(&self.observer_panicked),
            worker_panicked: load(&self.worker_panicked),
            admission_accepted: load(&self.admission_accepted),
            admission_partially_accepted: load(&self.admission_partially_accepted),
            admission_none_accepted: load(&self.admission_none_accepted),
            admission_no_destinations: load(&self.admission_no_destinations),
            admission_opaque_accepted: load(&self.admission_opaque_accepted),
            admission_dropped: load(&self.admission_dropped),
        }
    }

    /// Records the provider admission classification from one successful receipt.
    pub(super) fn record_admission(&self, outcome: AdmissionOutcome) {
        let counter = match outcome {
            AdmissionOutcome::Accepted(_) => &self.admission_accepted,
            AdmissionOutcome::PartiallyAccepted(_) => &self.admission_partially_accepted,
            AdmissionOutcome::NoneAccepted(_) => &self.admission_none_accepted,
            AdmissionOutcome::NoDestinations => &self.admission_no_destinations,
            AdmissionOutcome::OpaqueAccepted => &self.admission_opaque_accepted,
            AdmissionOutcome::Dropped => &self.admission_dropped,
        };
        Self::increment(counter);
    }

    /// Increments one counter without overflowing it.
    ///
    /// # Parameters
    /// - `counter`: the counter to increment using a saturating update.
    pub(super) fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::NotificationStats;
    use crate::model::AdmissionOutcome;
    use crate::model::AdmissionSummary;

    #[test]
    fn admission_classifications_are_recorded_independently_and_saturate() {
        let stats = NotificationStats::default();
        assert_eq!(0, stats.snapshot().admission_accepted());
        assert_eq!(0, stats.snapshot().admission_partially_accepted());
        assert_eq!(0, stats.snapshot().admission_none_accepted());
        assert_eq!(0, stats.snapshot().admission_no_destinations());
        assert_eq!(0, stats.snapshot().admission_opaque_accepted());
        assert_eq!(0, stats.snapshot().admission_dropped());

        stats.record_admission(AdmissionOutcome::Accepted(AdmissionSummary::default()));
        stats.record_admission(AdmissionOutcome::PartiallyAccepted(AdmissionSummary::default()));
        stats.record_admission(AdmissionOutcome::NoneAccepted(AdmissionSummary::default()));
        stats.record_admission(AdmissionOutcome::NoDestinations);
        stats.record_admission(AdmissionOutcome::OpaqueAccepted);
        stats.record_admission(AdmissionOutcome::Dropped);
        let snapshot = stats.snapshot();
        assert_eq!(1, snapshot.admission_accepted());
        assert_eq!(1, snapshot.admission_partially_accepted());
        assert_eq!(1, snapshot.admission_none_accepted());
        assert_eq!(1, snapshot.admission_no_destinations());
        assert_eq!(1, snapshot.admission_opaque_accepted());
        assert_eq!(1, snapshot.admission_dropped());

        stats
            .admission_accepted
            .store(u64::MAX, std::sync::atomic::Ordering::Relaxed);
        stats.record_admission(AdmissionOutcome::Accepted(AdmissionSummary::default()));
        let snapshot = stats.snapshot();
        assert_eq!(u64::MAX, snapshot.admission_accepted());
        assert_eq!(1, snapshot.admission_partially_accepted());
    }
}
