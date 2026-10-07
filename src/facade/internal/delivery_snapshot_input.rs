// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded scheduling metadata captured before the caller samples its clock.

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;

use crate::facade::DeliveryMetricsSnapshot;

/// One consistent set of live gauges and timestamped starts, without historical
/// delivery data. Callers capture this input before sampling a clock outside
/// the scheduler mutex.
pub(in crate::facade) struct DeliverySnapshotInput {
    /// Phase and lane gauges captured while holding the scheduler state mutex.
    gauges: DeliveryMetricsSnapshot,
    /// Starts of the selected currently owned leases, bounded by owned
    /// capacity.
    owned_starts: Vec<MonotonicInstant>,
}

impl DeliverySnapshotInput {
    /// Creates a capture from owned metadata; no clock or user callback is
    /// invoked.
    ///
    /// # Parameters
    /// - `gauges`: live scheduling gauges from the same locked state as the
    ///   starts.
    /// - `owned_starts`: selected live lease starts transferred from the state
    ///   scan.
    ///
    /// # Returns
    /// Immutable bounded input ready for a later external clock sample.
    #[must_use]
    #[inline]
    pub(super) fn new(gauges: DeliveryMetricsSnapshot, owned_starts: Vec<MonotonicInstant>) -> Self {
        Self { gauges, owned_starts }
    }

    /// Computes the oldest age from this capture after the caller samples its
    /// clock. Leases created after capture cannot affect these gauges or
    /// cause a time-order error.
    ///
    /// # Parameters
    /// - `now`: monotonic instant sampled after obtaining this capture, outside
    ///   scheduler locks.
    ///
    /// # Returns
    /// The captured live gauges and oldest timestamped lease age; no starts
    /// yields `None`.
    ///
    /// # Errors
    /// Returns `TimeError::ClockDomainMismatch` for a start from a foreign
    /// clock domain, or `TimeError::InvalidInstantOrder` when `now`
    /// precedes a captured start.
    #[must_use = "delivery snapshots should be observed"]
    pub(in crate::facade) fn at(self, now: MonotonicInstant) -> Result<DeliveryMetricsSnapshot, TimeError> {
        let mut snapshot = self.gauges;
        for started in self.owned_starts {
            let age = now.duration_since(started)?;
            snapshot.oldest_owned_age = Some(snapshot.oldest_owned_age.map_or(age, |oldest| oldest.max(age)));
        }
        Ok(snapshot)
    }
}
