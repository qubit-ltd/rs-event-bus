// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscription worker-context tracking.

use std::cell::RefCell;
use std::sync::Arc;

use super::super::local_event_bus_inner::LocalEventBusInner;

thread_local! {
    static SUBSCRIPTION_WORKER_BUS_IDS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Returns a stable in-process identifier for one local event bus inner value.
pub(super) fn local_event_bus_id(inner: &Arc<LocalEventBusInner>) -> usize {
    Arc::as_ptr(inner) as usize
}

/// Returns whether the current thread is processing work for the bus.
pub(super) fn is_current_subscription_worker_for_bus(bus_id: usize) -> bool {
    SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| bus_ids.borrow().contains(&bus_id))
}

/// Thread-local marker for subscriber worker execution.
pub(super) struct SubscriptionWorkerContext {
    bus_id: usize,
}

impl SubscriptionWorkerContext {
    /// Marks the current thread as processing subscriber work for a bus.
    pub(super) fn enter(bus_id: usize) -> Self {
        SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| {
            bus_ids.borrow_mut().push(bus_id);
        });
        Self { bus_id }
    }
}

impl Drop for SubscriptionWorkerContext {
    fn drop(&mut self) {
        SUBSCRIPTION_WORKER_BUS_IDS.with(|bus_ids| {
            let mut bus_ids = bus_ids.borrow_mut();
            if let Some(position) = bus_ids.iter().rposition(|bus_id| *bus_id == self.bus_id) {
                bus_ids.remove(position);
            }
        });
    }
}
