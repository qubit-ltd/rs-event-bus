// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded delivery admission and drop-based permit release.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Tracks a bounded number of accepted in-flight deliveries.
#[derive(Clone, Debug)]
pub(crate) struct AdmissionTracker {
    limit: usize,
    in_flight: Arc<AtomicUsize>,
}

impl AdmissionTracker {
    /// Creates a tracker with a positive in-flight limit.
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
    #[cfg(test)]
    pub(crate) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }
}

/// RAII reservation; dropping it releases exactly one admission slot.
#[derive(Debug)]
pub(crate) struct AdmissionPermit {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
