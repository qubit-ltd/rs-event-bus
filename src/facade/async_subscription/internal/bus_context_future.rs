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
///
/// # Type Parameters
///
/// - `F`: Future whose polls should carry the associated bus identity.
pub(in crate::facade) struct BusContextFuture<F: Future> {
    /// Identity of the facade associated with the wrapped operation.
    bus_key: usize,
    /// Caller-owned future pinned for the wrapper's lifetime.
    future: Option<Pin<Box<F>>>,
    /// Whether destroying the wrapped future should also carry this bus
    /// identity.
    scope_drop: bool,
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
            future: Some(Box::pin(future)),
            scope_drop: false,
        }
    }

    /// Pins a cleanup future whose cancellation and completion drops carry the
    /// bus identity.
    ///
    /// # Parameters
    /// - `bus_key`: facade identity used by reentrant-operation detection.
    /// - `future`: cleanup operation whose polls and destruction are scoped.
    ///
    /// # Returns
    /// A future wrapper that scopes the identity to polls and destruction.
    #[must_use = "futures do nothing unless they are polled"]
    pub(in crate::facade) fn new_with_drop_context(bus_key: usize, future: F) -> Self {
        Self {
            bus_key,
            future: Some(Box::pin(future)),
            scope_drop: true,
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
        let _scope = enter_bus_scope(this.bus_key);
        this.future
            .as_mut()
            .expect("future is present while polled")
            .as_mut()
            .poll(context)
    }
}

impl<F: Future> Drop for BusContextFuture<F> {
    /// Destroys the wrapped future, restoring its bus identity for cleanup
    /// paths.
    fn drop(&mut self) {
        let future = self.future.take();
        if self.scope_drop {
            let _scope = enter_bus_scope(self.bus_key);
            drop(future);
        } else {
            drop(future);
        }
    }
}

/// Pushes a bus identity and returns the guard that removes it.
///
/// # Parameters
/// - `bus_key`: facade identity to add to the current thread's scope stack.
///
/// # Returns
/// A guard that removes the identity when dropped.
fn enter_bus_scope(bus_key: usize) -> bus_poll_scope::BusPollScope {
    ACTIVE_BUS_POLLS.with(|active| active.borrow_mut().push(bus_key));
    bus_poll_scope::BusPollScope
}

/// Runs synchronous cleanup while the owning bus identity is active.
///
/// The scope covers both the action and destruction of its captured values.
///
/// # Type Parameters
/// - `R`: Result returned by the cleanup action.
///
/// # Parameters
/// - `bus_key`: Identity of the bus whose context is active during cleanup.
/// - `action`: Synchronous cleanup operation to run in that context.
///
/// # Returns
/// The value returned by `action`.
pub(in crate::facade) fn with_bus_scope<R>(bus_key: usize, action: impl FnOnce() -> R) -> R {
    let _scope = enter_bus_scope(bus_key);
    action()
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
#[must_use = "check whether this bus is currently being polled"]
#[inline]
pub(in crate::facade) fn is_current_bus_poll(bus_key: usize) -> bool {
    ACTIVE_BUS_POLLS.with(|active| active.borrow().contains(&bus_key))
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;

    use super::BusContextFuture;
    use super::is_current_bus_poll;

    struct CheckDropScope(Arc<std::sync::atomic::AtomicBool>, usize);

    impl Future for CheckDropScope {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for CheckDropScope {
        fn drop(&mut self) {
            self.0
                .store(is_current_bus_poll(self.1), std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn test_cleanup_future_drop_carries_bus_scope() {
        let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bus_key = 17;
        let mut future = Box::pin(BusContextFuture::new_with_drop_context(
            bus_key,
            CheckDropScope(observed.clone(), bus_key),
        ));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
        drop(future);
        assert!(observed.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!is_current_bus_poll(bus_key));
    }
}
