// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Poll-scoped bus identity for reentrant deadlock detection.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

mod bus_poll_scope;

thread_local! {
    /// Stack of facade identities whose futures are being polled on this thread.
    static ACTIVE_BUS_POLLS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Future wrapper that marks its owning bus during every poll.
pub(in crate::facade) struct BusContextFuture<F: Future> {
    /// Identity of the facade associated with the wrapped operation.
    bus_key: usize,
    /// Caller-owned future pinned for the wrapper's lifetime.
    future: Pin<Box<F>>,
}

impl<F: Future> BusContextFuture<F> {
    /// Pins a future and associates it with a facade identity.
    pub(in crate::facade) fn new(bus_key: usize, future: F) -> Self {
        Self {
            bus_key,
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for BusContextFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        ACTIVE_BUS_POLLS.with(|active| active.borrow_mut().push(this.bus_key));
        let _scope = bus_poll_scope::BusPollScope;
        this.future.as_mut().poll(context)
    }
}

/// Reports whether the current thread is polling an operation for this bus.
pub(in crate::facade) fn is_current_bus_poll(bus_key: usize) -> bool {
    ACTIVE_BUS_POLLS.with(|active| active.borrow().contains(&bus_key))
}
