// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::facade::async_subscription::AsyncAdmissionPermit;
use crate::facade::async_subscription::internal::pending_delivery::PendingDelivery;

/// Event returned while waiting for a delivery permit.
pub(in crate::facade::async_subscription) enum AdmissionWaitEvent<T: 'static> {
    /// Admission capacity became available.
    Permit(AsyncAdmissionPermit),
    /// A delivery task completed while capacity was unavailable.
    Delivery(Box<PendingDelivery<T>>),
    /// Immediate shutdown requested that no new task start.
    ImmediateStop,
}
