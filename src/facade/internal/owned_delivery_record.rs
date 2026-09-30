// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Metadata retained for the lifetime of one delivery lease.

use qubit_clock::MonotonicInstant;
use qubit_id::Id;

use super::owned_delivery_phase::OwnedDeliveryPhase;
use crate::pipeline::OrderingLaneKey;

/// One ownership credit; never contains a receiver, payload, or future.
pub(super) struct OwnedDeliveryRecord {
    /// Registered owner of this delivery.
    pub(super) subscription_id: Id,
    /// Current ownership and execution phase.
    pub(super) phase: OwnedDeliveryPhase,
    /// First owner-sampled receive start; unclaimed reservations have no
    /// timestamp.
    pub(super) created_at: Option<MonotonicInstant>,
    /// Ordered lane; absent means this delivery is independent.
    pub(super) lane: Option<OrderingLaneKey>,
}
