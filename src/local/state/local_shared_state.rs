// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared provider state retained by local SPI handles.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;

use super::bus_state::BusState;
use crate::local::async_signal::AsyncSignal;
use crate::local::outstanding_budget::OutstandingBudget;

/// Shared router retained by the SPI and all active subscription receivers.
pub(in crate::local) struct LocalSharedState {
    /// Routing metadata, admission gate, and shutdown result.
    pub(in crate::local) state: Mutex<BusState>,
    /// Signals provider-level queue or settlement progress.
    pub(in crate::local) changed: Condvar,
    /// Wakes asynchronous provider progress waiters.
    pub(in crate::local) async_changed: AsyncSignal,
    /// Serializes concurrent shutdown callers through the final outcome.
    pub(in crate::local) shutdown_gate: Mutex<()>,
    /// Pending message bound copied into each new queue.
    pub(in crate::local) capacity: usize,
    /// Provider-wide bound shared by all destination queues.
    pub(in crate::local) outstanding: OutstandingBudget,
}

impl LocalSharedState {
    /// Creates an empty provider state with the configured queue and
    /// outstanding bounds.
    ///
    /// # Parameters
    /// - `capacity`: maximum queued and unsettled items for each subscription.
    /// - `max_total_outstanding`: provider-wide outstanding-delivery bound.
    /// - `max_weight`: optional positive declared-weight budget in bytes.
    ///
    /// # Returns
    /// Shared provider state with no queues or deliveries.
    ///
    /// # Panics
    /// Panics if `max_total_outstanding` is zero.
    pub(in crate::local) fn new(
        capacity: usize,
        max_total_outstanding: usize,
        max_weight: Option<NonZeroUsize>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BusState::default()),
            changed: Condvar::new(),
            async_changed: AsyncSignal::default(),
            shutdown_gate: Mutex::new(()),
            capacity,
            outstanding: OutstandingBudget::new(max_total_outstanding, max_weight),
        })
    }
}
