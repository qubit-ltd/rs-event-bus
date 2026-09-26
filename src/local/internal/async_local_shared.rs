// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared capacity, registry, notification, and timer state for the async bus.

use std::sync::Arc;
use std::sync::Mutex;

use qubit_clock::Timer;

use super::super::async_signal::AsyncSignal;
use super::super::outstanding_budget::OutstandingBudget;
use super::AsyncBusState;

pub(in crate::local) struct AsyncLocalShared {
    capacity: usize,
    pub(in crate::local) outstanding: OutstandingBudget,
    pub(in crate::local) state: Mutex<AsyncBusState>,
    pub(in crate::local) changed: AsyncSignal,
    pub(in crate::local) timer: Arc<dyn Timer>,
}

impl AsyncLocalShared {
    pub(in crate::local) fn new(capacity: usize, max_total_outstanding: usize, timer: Arc<dyn Timer>) -> Self {
        Self {
            capacity,
            outstanding: OutstandingBudget::new(max_total_outstanding),
            state: Mutex::new(AsyncBusState::default()),
            changed: AsyncSignal::default(),
            timer,
        }
    }

    pub(in crate::local) fn capacity(&self) -> usize {
        self.capacity
    }
}
