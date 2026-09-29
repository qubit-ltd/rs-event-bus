// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded delivery admission and drop-based permit release.

#[path = "admission/internal/mod.rs"]
mod internal;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub(crate) use internal::AdmissionPermit;

/// Tracks a bounded number of accepted in-flight deliveries.
#[derive(Clone, Debug)]
pub(crate) struct AdmissionTracker {
    /// Maximum number of outstanding permits allowed by this tracker.
    limit: usize,
    /// Shared atomic count retained by every outstanding permit.
    in_flight: Arc<AtomicUsize>,
}

impl AdmissionTracker {
    /// Creates a tracker with a positive in-flight limit.
    ///
    /// # Parameters
    /// - `limit`: maximum number of permits that may be held at once.
    ///
    /// # Returns
    /// A tracker configured with the requested capacity.
    ///
    /// # Errors
    /// Returns an error when `limit` is zero.
    pub(crate) fn new(limit: usize) -> Result<Self, &'static str> {
        if limit == 0 {
            return Err("admission limit must be greater than zero");
        }
        Ok(Self {
            limit,
            in_flight: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Attempts to reserve one in-flight slot without blocking.
    ///
    /// # Returns
    /// A permit if capacity is available, or `None` when the limit is reached.
    pub(crate) fn try_acquire(&self) -> Option<AdmissionPermit> {
        self.in_flight
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < self.limit).then_some(current + 1)
            })
            .ok()
            .map(|_| AdmissionPermit {
                in_flight: self.in_flight.clone(),
            })
    }

    /// Returns the number of currently held permits.
    ///
    /// # Returns
    /// The current permit count, read with acquire ordering.
    #[must_use]
    #[inline]
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}
