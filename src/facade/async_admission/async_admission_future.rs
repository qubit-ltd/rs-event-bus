// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation-safe future for acquiring an asynchronous delivery slot.

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use super::AsyncAdmission;
use super::async_admission_permit::AsyncAdmissionPermit;

/// Waits in FIFO order for bus-wide delivery admission.
pub(in crate::facade) struct AsyncAdmissionFuture {
    /// Shared gate whose waiter queue this future owns.
    admission: Arc<AsyncAdmission>,
    /// Stable queue identity removed if this future is cancelled.
    waiter_id: u64,
    /// Whether this future currently occupies a queue position.
    queued: bool,
}

impl AsyncAdmissionFuture {
    /// Creates an unqueued waiter with an identity assigned by the gate.
    ///
    /// # Parameters
    /// - `admission`: shared gate whose FIFO queue this waiter will use.
    /// - `waiter_id`: unique queue identity assigned by the gate.
    ///
    /// # Returns
    /// A future that has not yet registered its waker or queue position.
    pub(super) fn new(admission: Arc<AsyncAdmission>, waiter_id: u64) -> Self {
        Self {
            admission,
            waiter_id,
            queued: false,
        }
    }
}

impl Future for AsyncAdmissionFuture {
    /// Admission permit for the acquired slot.
    type Output = AsyncAdmissionPermit;

    /// Registers or refreshes this waiter's waker and admits it when it is the
    /// queue head and a slot is available.
    ///
    /// # Parameters
    /// - `self`: pinned mutable future state.
    /// - `context`: task context supplying the current waker.
    ///
    /// # Returns
    /// `Poll::Ready` with an owned permit when admitted, otherwise
    /// `Poll::Pending`.
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
            Poll::Ready(AsyncAdmissionPermit::new(this.admission.clone()))
        } else {
            Poll::Pending
        }
    }
}

impl Drop for AsyncAdmissionFuture {
    /// Removes a queued waiter and wakes the next eligible future if needed.
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
