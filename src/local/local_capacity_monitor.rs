// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Read-only monitoring for a local provider's shared outstanding budget.

use std::sync::Arc;

use super::LocalCapacitySnapshot;
use super::outstanding_budget::OutstandingBudget;

/// Cloneable read-only handle to a local provider capacity budget.
///
/// Obtain it with `EventBus::local_with_monitor`,
/// `EventBus::local_with_config_and_monitor`,
/// `AsyncEventBus::local_with_monitor`, or
/// `AsyncEventBus::local_with_config_and_monitor`. Registry-created providers
/// do not expose this handle. Clones observe the same budget. The handle keeps
/// only accounting state alive, not the provider, queues, subscriptions, or
/// payloads.
///
/// Snapshot reads briefly acquire the budget mutex.
///
/// ```
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let (bus, monitor) = EventBus::local_with_monitor(LocalEventBusConfig::new())?;
/// let snapshot = monitor.snapshot();
/// assert_eq!(snapshot.outstanding_count, 0);
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct LocalCapacityMonitor {
    /// Shared provider-wide integer accounting state.
    budget: Arc<OutstandingBudget>,
    /// Configured pending plus in-flight capacity for each subscription.
    queue_capacity: usize,
}

impl LocalCapacityMonitor {
    pub(super) fn new(budget: Arc<OutstandingBudget>, queue_capacity: usize) -> Self {
        Self { budget, queue_capacity }
    }

    /// Returns a consistent copy of local capacity usage and target rejection
    /// totals at one budget-lock observation point.
    ///
    /// This synchronous read briefly waits for the budget mutex. It does not
    /// guarantee capacity for a later publish and is not a hard real-time read.
    #[must_use]
    pub fn snapshot(&self) -> LocalCapacitySnapshot {
        self.budget.snapshot(self.queue_capacity)
    }
}
