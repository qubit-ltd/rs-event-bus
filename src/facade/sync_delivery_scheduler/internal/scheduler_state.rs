// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Granted callback queue owned by the synchronous handler pool.

use std::collections::HashSet;
use std::collections::VecDeque;

use qubit_id::Id;

/// Pool lifecycle is independent of the delivery core's owned leases.
#[derive(Default)]
pub(in crate::facade::sync_delivery_scheduler) struct SchedulerState {
    /// Jobs already holding a core execution grant; bounded by H.
    pub(in crate::facade::sync_delivery_scheduler) jobs: VecDeque<Box<dyn FnOnce() + Send + 'static>>,
    /// Graceful shutdown keeps dispatching already owned deliveries.
    pub(in crate::facade::sync_delivery_scheduler) draining: bool,
    /// Immediate shutdown permanently strengthens a graceful request.
    pub(in crate::facade::sync_delivery_scheduler) immediate: bool,
    /// Individually canceled owners never drain new handlers.
    pub(in crate::facade::sync_delivery_scheduler) cancelled: HashSet<Id>,
    /// Allows idle pool workers to exit after owners finish.
    pub(in crate::facade::sync_delivery_scheduler) stopped: bool,
}
