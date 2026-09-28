// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fair bus-wide admission for asynchronous deliveries.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

/// Bus-wide asynchronous delivery admission. Waiters are woken whenever a
/// delivery releases its permit; cancellation removes the waiter by RAII.
pub(super) struct AsyncAdmission {
    limit: usize,
    state: Mutex<AsyncAdmissionState>,
    next_waiter: AtomicU64,
}

#[derive(Default)]
struct AsyncAdmissionState {
    in_flight: usize,
    waiters: VecDeque<(u64, Waker)>,
}

impl AsyncAdmission {
    pub(super) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            state: Mutex::new(AsyncAdmissionState::default()),
            next_waiter: AtomicU64::new(1),
        })
    }

    /// Returns a future that waits for the next bus-wide delivery slot.
    pub(super) fn acquire(self: &Arc<Self>) -> AsyncAdmissionFuture {
        AsyncAdmissionFuture {
            admission: self.clone(),
            waiter_id: self.next_waiter.fetch_add(1, Ordering::Relaxed),
            queued: false,
        }
    }
}

/// Cancellation-safe future for acquiring an async delivery slot.
pub(super) struct AsyncAdmissionFuture {
    admission: Arc<AsyncAdmission>,
    waiter_id: u64,
    queued: bool,
}

impl Future for AsyncAdmissionFuture {
    type Output = AsyncAdmissionPermit;

    fn poll(mut self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        let (admitted, next_waker) = {
            let mut state = this
                .admission
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !this.queued {
                state.waiters.push_back((this.waiter_id, context.waker().clone()));
                this.queued = true;
            } else if let Some((_, waker)) = state.waiters.iter_mut().find(|(id, _)| *id == this.waiter_id)
                && !waker.will_wake(context.waker())
            {
                *waker = context.waker().clone();
            }
            let is_head = state.waiters.front().is_some_and(|(id, _)| *id == this.waiter_id);
            if is_head && state.in_flight < this.admission.limit {
                state.waiters.pop_front();
                state.in_flight += 1;
                this.queued = false;
                let next_waker = (state.in_flight < this.admission.limit)
                    .then(|| state.waiters.front().map(|(_, waker)| waker.clone()))
                    .flatten();
                (true, next_waker)
            } else {
                (false, None)
            }
        };
        if let Some(waker) = next_waker {
            waker.wake();
        }
        if admitted {
            Poll::Ready(AsyncAdmissionPermit {
                admission: this.admission.clone(),
            })
        } else {
            Poll::Pending
        }
    }
}

impl Drop for AsyncAdmissionFuture {
    fn drop(&mut self) {
        if self.queued {
            let waker = {
                let mut state = self
                    .admission
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let was_head = state.waiters.front().is_some_and(|(id, _)| *id == self.waiter_id);
                state.waiters.retain(|(id, _)| *id != self.waiter_id);
                (was_head && state.in_flight < self.admission.limit)
                    .then(|| state.waiters.front().map(|(_, waker)| waker.clone()))
                    .flatten()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}

/// RAII permit covering one received delivery through terminal settlement.
pub(super) struct AsyncAdmissionPermit {
    admission: Arc<AsyncAdmission>,
}

impl Drop for AsyncAdmissionPermit {
    fn drop(&mut self) {
        let wakers = {
            let mut state = self
                .admission
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_flight = state.in_flight.saturating_sub(1);
            state
                .waiters
                .front()
                .map(|(_, waker)| waker.clone())
                .into_iter()
                .collect::<Vec<_>>()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
