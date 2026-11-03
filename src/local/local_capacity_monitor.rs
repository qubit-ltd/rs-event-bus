// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Read-only monitoring for a local provider's shared outstanding budget.

use std::sync::Arc;

use super::LocalCapacitySnapshot;
use super::outstanding_budget::OutstandingBudget;

/// Cloneable read-only handle to a local provider capacity budget.
///
/// The handle retains only integer accounting state and per-subscription
/// capacity. It does not keep queues, subscriptions, payloads, or the provider
/// lifecycle alive. Snapshot reads briefly acquire the budget mutex.
#[derive(Clone)]
pub struct LocalCapacityMonitor {
    budget: Arc<OutstandingBudget>,
    queue_capacity: usize,
}

impl LocalCapacityMonitor {
    pub(super) fn new(budget: Arc<OutstandingBudget>, queue_capacity: usize) -> Self {
        Self { budget, queue_capacity }
    }

    /// Returns a consistent copy of local capacity usage and target rejection
    /// totals at one budget-lock observation point.
    #[must_use]
    pub fn snapshot(&self) -> LocalCapacitySnapshot {
        self.budget.snapshot(self.queue_capacity)
    }
}
