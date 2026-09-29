// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Atomic limit on concurrent synchronous subscription coordinator threads.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::subscription_worker_permit::SubscriptionWorkerPermit;

/// Tracks active and reserved subscription coordinator worker slots.
pub(in crate::facade) struct SubscriptionWorkerBudget {
    /// Current number of running or reserved worker threads.
    pub(in crate::facade) active: AtomicUsize,
    /// Maximum number of subscription workers.
    pub(in crate::facade) limit: usize,
}

impl SubscriptionWorkerBudget {
    /// Reserves one worker slot when the configured limit allows it.
    ///
    /// # Returns
    /// A permit when capacity exists, otherwise None.
    #[must_use]
    pub(in crate::facade) fn try_reserve(self: &Arc<Self>) -> Option<SubscriptionWorkerPermit> {
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if active >= self.limit {
                return None;
            }
            match self
                .active
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Some(SubscriptionWorkerPermit(self.clone())),
                Err(observed) => active = observed,
            }
        }
    }
}
