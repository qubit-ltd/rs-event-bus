// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fixed-size counters for publication outcomes.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::PublishMetricsSnapshot;
use crate::model::PublishAcknowledgement;
use crate::model::PublishReceipt;

/// Per-facade publication counters shared by facade clones.
#[derive(Default)]
pub(crate) struct PublishMetrics {
    attempts: AtomicU64,
    errors: AtomicU64,
    dropped: AtomicU64,
    opaque_accepted: AtomicU64,
    zero_destinations: AtomicU64,
    accepted_destinations: AtomicU64,
    filtered_destinations: AtomicU64,
    rejected_destinations: AtomicU64,
}

impl PublishMetrics {
    /// Records one public publish attempt.
    pub(crate) fn record_attempt(&self) {
        Self::increment(&self.attempts, 1);
    }

    /// Records one failed public call.
    pub(crate) fn record_error(&self) {
        Self::increment(&self.errors, 1);
    }

    /// Records one successful public call and its provider-reported outcome.
    pub(crate) fn record_receipt(&self, receipt: &PublishReceipt) {
        match receipt.acknowledgement() {
            PublishAcknowledgement::DroppedByInterceptor => Self::increment(&self.dropped, 1),
            PublishAcknowledgement::Accepted { .. } => Self::increment(&self.opaque_accepted, 1),
            PublishAcknowledgement::DestinationAdmissions(_) => {
                if let Some(summary) = receipt.admission_summary() {
                    if summary.accepted == 0 && summary.filtered == 0 && summary.rejected == 0 {
                        Self::increment(&self.zero_destinations, 1);
                    }
                    Self::increment(&self.accepted_destinations, summary.accepted as u64);
                    Self::increment(&self.filtered_destinations, summary.filtered as u64);
                    Self::increment(&self.rejected_destinations, summary.rejected as u64);
                }
            }
        }
    }

    /// Loads each counter independently using relaxed ordering.
    pub(crate) fn snapshot(&self) -> PublishMetricsSnapshot {
        PublishMetricsSnapshot {
            attempts: self.attempts.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            opaque_accepted: self.opaque_accepted.load(Ordering::Relaxed),
            zero_destinations: self.zero_destinations.load(Ordering::Relaxed),
            accepted_destinations: self.accepted_destinations.load(Ordering::Relaxed),
            filtered_destinations: self.filtered_destinations.load(Ordering::Relaxed),
            rejected_destinations: self.rejected_destinations.load(Ordering::Relaxed),
        }
    }

    fn increment(counter: &AtomicU64, amount: u64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(amount))
        });
    }
}
