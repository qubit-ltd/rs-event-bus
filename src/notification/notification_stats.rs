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

/// Atomic counters shared by the publisher handle and its worker.
#[derive(Default)]
pub(super) struct NotificationStats {
    pub(super) enqueued: AtomicU64,
    pub(super) queue_full: AtomicU64,
    pub(super) queue_closed: AtomicU64,
    pub(super) published: AtomicU64,
    pub(super) publish_errors: AtomicU64,
    pub(super) request_errors: AtomicU64,
    pub(super) observer_panicked: AtomicU64,
    pub(super) worker_panicked: AtomicU64,
}

impl NotificationStats {
    /// Loads all counters into one best-effort snapshot.
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
        }
    }

    /// Increments one counter without overflowing it.
    pub(super) fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }
}
