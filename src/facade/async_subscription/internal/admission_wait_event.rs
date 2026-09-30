// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous subscription state.

use super::PendingDelivery;
use crate::facade::async_admission::AsyncAdmissionPermit;

/// Event returned while waiting for a delivery permit.
///
/// # Type Parameters
/// - `T`: payload type retained by a completed delivery.
#[must_use = "handle the delivery event or admission permit before continuing"]
pub(in crate::facade::async_subscription) enum AdmissionWaitEvent<T: 'static> {
    /// Admission capacity became available.
    Permit(
        /// Slot retained for the pending delivery.
        AsyncAdmissionPermit,
    ),
    /// A delivery task completed while capacity was unavailable.
    Delivery(
        /// Completed handler task awaiting terminal settlement.
        Box<PendingDelivery<T>>,
    ),
    /// Immediate shutdown requested that no new task start.
    ImmediateStop,
}
