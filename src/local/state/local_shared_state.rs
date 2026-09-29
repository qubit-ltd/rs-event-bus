// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared provider state retained by local SPI handles.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use super::bus_state::BusState;

/// Shared router retained by the SPI and all active subscription receivers.
pub(in crate::local) struct LocalSharedState {
    /// Routing metadata, admission gate, and shutdown result.
    pub(in crate::local) state: Mutex<BusState>,
    /// Signals provider-level queue or settlement progress.
    pub(in crate::local) changed: Condvar,
    /// Wakes asynchronous provider progress waiters.
    pub(in crate::local) async_changed: crate::local::async_signal::AsyncSignal,
    /// Serializes concurrent shutdown callers through the final outcome.
    pub(in crate::local) shutdown_gate: Mutex<()>,
    /// Pending message bound copied into each new queue.
    pub(in crate::local) capacity: usize,
    /// Provider-wide bound shared by all destination queues.
    pub(in crate::local) outstanding: crate::local::outstanding_budget::OutstandingBudget,
}

impl LocalSharedState {
    /// Creates an empty provider state with a validated positive queue bound.
    ///
    /// # Parameters
    /// - `capacity`: maximum queued and unsettled items for each subscription.
    /// - `max_total_outstanding`: provider-wide outstanding-delivery bound.
    ///
    /// # Returns
    /// Shared provider state with no queues or deliveries.
    ///
    /// # Panics
    /// Panics if `max_total_outstanding` is zero.
    pub(in crate::local) fn new(capacity: usize, max_total_outstanding: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BusState::default()),
            changed: Condvar::new(),
            async_changed: crate::local::async_signal::AsyncSignal::default(),
            shutdown_gate: Mutex::new(()),
            capacity,
            outstanding: crate::local::outstanding_budget::OutstandingBudget::new(max_total_outstanding),
        })
    }
}
