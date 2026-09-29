// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused owner for local provider state.

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;

use qubit_id::Id;

use super::LocalQueueState;
use crate::spi::TopicAddress;

/// One bounded FIFO queue and receiver lifecycle for a logical subscriber.
pub(in crate::local) struct LocalQueue {
    /// Bus-local identifier used to bind settlement tokens.
    pub(in crate::local) id: Id,
    /// Topic this queue receives.
    pub(in crate::local) topic: TopicAddress,
    /// Logical subscriber identity reported by publish admissions.
    pub(in crate::local) subscriber_id: crate::model::SubscriberId,
    /// Maximum number of queued and unsettled events for this subscription.
    pub(in crate::local) capacity: usize,
    /// Queue state shared by publisher and its single receiver.
    pub(in crate::local) state: Mutex<LocalQueueState>,
    /// Wakes synchronous receives after publish, close, or shutdown.
    pub(in crate::local) ready: Condvar,
    /// Wakes asynchronous receives after provider state changes.
    pub(in crate::local) async_ready: crate::local::async_signal::AsyncSignal,
}

impl LocalQueue {
    /// Locks queue state while recovering from a poisoned standard mutex.
    ///
    /// # Returns
    /// The queue state guard, recovering the inner state if the mutex was
    /// poisoned.
    pub(in crate::local) fn lock(&self) -> std::sync::MutexGuard<'_, LocalQueueState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}
