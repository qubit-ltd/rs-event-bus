// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! RAII reservation for one synchronous subscription worker.

use crate::facade::event_bus::Arc;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::SubscriptionWorkerBudget;

pub(in crate::facade) struct SubscriptionWorkerPermit(Arc<SubscriptionWorkerBudget>);

impl SubscriptionWorkerPermit {
    pub(in crate::facade) fn new(budget: Arc<SubscriptionWorkerBudget>) -> Self {
        Self(budget)
    }
}

impl Drop for SubscriptionWorkerPermit {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
    }
}
