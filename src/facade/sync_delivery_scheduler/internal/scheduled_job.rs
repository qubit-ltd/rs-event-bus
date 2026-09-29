// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Accepted handler closure and resources retained until it exits.

use qubit_id::Id;

use super::super::OrderingLaneKey;
use crate::pipeline::AdmissionPermit;

/// One accepted handler task retaining admission until its closure returns.
pub(in crate::facade::sync_delivery_scheduler) struct ScheduledJob {
    /// Subscription coordinator that owns this queued delivery.
    pub(in crate::facade::sync_delivery_scheduler) subscription_id: Id,
    /// Optional lane that must remain exclusive while the job runs.
    pub(in crate::facade::sync_delivery_scheduler) ordering_key: Option<OrderingLaneKey>,
    /// Handler closure called with whether cancellation won before start.
    pub(in crate::facade::sync_delivery_scheduler) run: Box<dyn FnOnce(bool) + Send + 'static>,
    /// Global admission slot retained across queueing and execution.
    pub(in crate::facade::sync_delivery_scheduler) permit: Option<AdmissionPermit>,
}
