// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop-based reservation of one subscription worker slot.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::subscription_worker_budget::SubscriptionWorkerBudget;

/// Keeps a worker slot reserved until the worker exits.
#[must_use = "the worker permit must stay alive until the worker exits"]
pub(in crate::facade) struct SubscriptionWorkerPermit(
    /// Budget whose active worker reservation is released on drop.
    pub(in crate::facade) Arc<SubscriptionWorkerBudget>,
);

impl Drop for SubscriptionWorkerPermit {
    /// Releases the reserved worker slot.
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
