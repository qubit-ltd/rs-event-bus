// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous activity tracking and wake registrations.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// A signal that wakes tasks waiting for asynchronous facade state changes.
#[derive(Default)]
pub(in crate::facade) struct AsyncSignal {
    wakers: Mutex<HashMap<u64, std::task::Waker>>,
    next_waiter: AtomicU64,
}

impl AsyncSignal {
    pub(in crate::facade) fn notify(&self) {
        let wakers = std::mem::take(&mut *self.wakers.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        for (_, waker) in wakers {
            waker.wake();
        }
    }

    pub(in crate::facade) fn register_waiter(&self, id: u64, waker: &std::task::Waker) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, waker.clone());
    }

    pub(in crate::facade) fn unregister(&self, id: u64) {
        self.wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    pub(in crate::facade) fn next_waiter_id(&self) -> u64 {
        self.next_waiter.fetch_add(1, Ordering::Relaxed)
    }
}

/// Counts active facade operations and received deliveries by topic.
#[derive(Default)]
pub(in crate::facade) struct AsyncTracker {
    state: Mutex<TrackerState>,
    pub(in crate::facade) signal: AsyncSignal,
}

#[derive(Default)]
struct TrackerState {
    active_runners: usize,
    active_publishes: usize,
    active_subscribes: usize,
    active_closes: usize,
    in_flight: HashMap<Box<str>, usize>,
}

impl AsyncTracker {
    pub(in crate::facade) fn close_started(self: &Arc<Self>) -> AsyncCloseGuard {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_closes += 1;
        AsyncCloseGuard(self.clone())
    }

    pub(in crate::facade) fn runner_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_runners += 1;
    }

    pub(in crate::facade) fn runner_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners = state.active_runners.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    pub(in crate::facade) fn publish_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_publishes += 1;
    }

    pub(in crate::facade) fn publish_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_publishes = state.active_publishes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    pub(in crate::facade) fn subscribe_started(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_subscribes += 1;
    }

    pub(in crate::facade) fn subscribe_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_subscribes = state.active_subscribes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    pub(in crate::facade) fn close_finished(&self) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_closes = state.active_closes.saturating_sub(1);
        drop(state);
        self.signal.notify();
    }

    pub(in crate::facade) fn track(self: &Arc<Self>, topic: &str) -> AsyncDeliveryGuard {
        *self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .entry(topic.into())
            .or_default() += 1;
        AsyncDeliveryGuard {
            tracker: self.clone(),
            topic: topic.into(),
        }
    }

    pub(in crate::facade) fn is_idle(&self, topic: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .in_flight
            .get(topic)
            .copied()
            .unwrap_or(0)
            == 0
    }

    pub(in crate::facade) fn runners_stopped(&self) -> bool {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active_runners == 0
            && state.active_publishes == 0
            && state.active_subscribes == 0
            && state.active_closes == 0
    }
}

pub(in crate::facade) struct AsyncPublishGuard(Arc<AsyncTracker>);

impl AsyncPublishGuard {
    pub(in crate::facade) fn after_start(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncPublishGuard {
    fn drop(&mut self) {
        self.0.publish_finished();
    }
}

pub(in crate::facade) struct AsyncSubscribeGuard(Arc<AsyncTracker>);

impl AsyncSubscribeGuard {
    pub(in crate::facade) fn after_start(tracker: Arc<AsyncTracker>) -> Self {
        Self(tracker)
    }
}

impl Drop for AsyncSubscribeGuard {
    fn drop(&mut self) {
        self.0.subscribe_finished();
    }
}

pub(in crate::facade) struct AsyncRunnerGuard(Arc<AsyncTracker>);

impl AsyncRunnerGuard {
    pub(in crate::facade) fn enter(tracker: Arc<AsyncTracker>) -> Self {
        tracker.runner_started();
        Self(tracker)
    }
}

impl Drop for AsyncRunnerGuard {
    fn drop(&mut self) {
        self.0.runner_finished();
    }
}

pub(in crate::facade) struct AsyncCloseGuard(Arc<AsyncTracker>);

impl Drop for AsyncCloseGuard {
    fn drop(&mut self) {
        self.0.close_finished();
    }
}

pub(in crate::facade) struct AsyncDeliveryGuard {
    tracker: Arc<AsyncTracker>,
    topic: Box<str>,
}

impl Drop for AsyncDeliveryGuard {
    fn drop(&mut self) {
        let mut state = self
            .tracker
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = state.in_flight.get_mut(self.topic.as_ref()) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.in_flight.remove(self.topic.as_ref());
            }
        }
        drop(state);
        self.tracker.signal.notify();
    }
}
