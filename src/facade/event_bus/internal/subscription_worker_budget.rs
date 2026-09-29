// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal sync facade state owner.

use crate::facade::event_bus::Arc;
use crate::facade::event_bus::AtomicUsize;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::internal::SubscriptionWorkerPermit;

pub(in crate::facade) struct SubscriptionWorkerBudget {
    pub(in crate::facade) active: AtomicUsize,
    pub(in crate::facade) limit: usize,
}

impl SubscriptionWorkerBudget {
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
                Ok(_) => return Some(SubscriptionWorkerPermit::new(self.clone())),
                Err(observed) => active = observed,
            }
        }
    }
}
