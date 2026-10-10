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
    ///
    /// # Parameters
    ///
    /// - `bus_key`: Identity used to detect reentrant operations on the facade.
    /// - `future`: Operation whose polls should carry that identity.
    ///
    /// # Returns
    ///
    /// A future wrapper that scopes the identity to each poll.
    #[must_use = "futures do nothing unless they are polled"]
    pub(in crate::facade) fn new(bus_key: usize, future: F) -> Self {
        Self {
            bus_key,
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for BusContextFuture<F> {
    /// Output produced by the wrapped operation.
    type Output = F::Output;

    /// Polls the wrapped future while this facade identity is on the
    /// thread-local stack.
    ///
    /// # Parameters
    /// - `self`: Pinned context future being polled.
    /// - `context`: Task context used to poll the wrapped future.
    ///
    /// # Returns
    /// The wrapped future's pending or ready state.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        ACTIVE_BUS_POLLS.with(|active| active.borrow_mut().push(this.bus_key));
        let _scope = bus_poll_scope::BusPollScope;
        this.future.as_mut().poll(context)
    }
}

/// Reports whether the current thread is polling an operation for this bus.
///
/// # Parameters
///
/// - `bus_key`: Facade identity to search for in the current thread's poll
///   stack.
///
/// # Returns
///
/// `true` if that identity is present; otherwise, `false`.
#[must_use = "Use the returned query result."]
#[inline]
pub(in crate::facade) fn is_current_bus_poll(bus_key: usize) -> bool {
    ACTIVE_BUS_POLLS.with(|active| active.borrow().contains(&bus_key))
}
