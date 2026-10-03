// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared capacity, registry, notification, and timer state for the async bus.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_clock::Timer;

use super::super::async_signal::AsyncSignal;
use super::super::outstanding_budget::OutstandingBudget;
use super::AsyncBusState;

/// Capacity, registry, notification, and timer state shared by the async bus.
pub(in crate::local) struct AsyncLocalShared {
    /// Queue capacity copied into each mailbox.
    capacity: usize,
    /// Provider-wide bound on queued and unsettled deliveries.
    pub(in crate::local) outstanding: OutstandingBudget,
    /// Mailbox registry and provider lifecycle state.
    pub(in crate::local) state: Mutex<AsyncBusState>,
    /// Wakes graceful shutdown when queue state changes.
    pub(in crate::local) changed: AsyncSignal,
    /// Timer used by cancellation-safe provider waits.
    pub(in crate::local) timer: Arc<dyn Timer>,
}

impl AsyncLocalShared {
    /// Creates shared provider state with validated positive capacity limits.
    ///
    /// # Parameters
    /// - `capacity`: per-subscription queued and unsettled-message bound.
    /// - `max_total_outstanding`: provider-wide outstanding-message bound.
    /// - `max_weight`: optional positive declared-weight budget in bytes.
    /// - `timer`: runtime-neutral timer used by async operations.
    ///
    /// # Returns
    /// Shared state with an empty mailbox registry.
    ///
    /// # Panics
    /// Panics if `max_total_outstanding` is zero.
    #[must_use]
    pub(in crate::local) fn new(
        capacity: usize,
        max_total_outstanding: usize,
        max_weight: Option<NonZeroUsize>,
        timer: Arc<dyn Timer>,
    ) -> Self {
        Self {
            capacity,
            outstanding: OutstandingBudget::new(max_total_outstanding, max_weight),
            state: Mutex::new(AsyncBusState::default()),
            changed: AsyncSignal::default(),
            timer,
        }
    }

    /// Returns the per-subscription queue capacity.
    ///
    /// # Returns
    /// Maximum number of queued or unsettled deliveries in one mailbox.
    #[must_use]
    #[inline]
    pub(in crate::local) fn capacity(&self) -> usize {
        self.capacity
    }
}
