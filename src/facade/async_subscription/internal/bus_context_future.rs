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
    static ACTIVE_BUS_POLLS: RefCell<Vec<(usize, Option<qubit_id::Id>)>> = const { RefCell::new(Vec::new()) };
}

/// Future wrapper that marks its owning bus during every poll.
///
/// # Type Parameters
///
/// - `F`: Future whose polls should carry the associated bus identity.
pub(in crate::facade) struct BusContextFuture<F: Future> {
    /// Identity of the facade associated with the wrapped operation.
    bus_key: usize,
    /// Optional bus-local subscription identity carried by this scope.
    subscription_id: Option<qubit_id::Id>,
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
            subscription_id: None,
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
            subscription_id: None,
            future: Some(Box::pin(future)),
            scope_drop: true,
        }
    }

    /// Pins a future and associates it with one subscription identity.
    ///
    /// # Parameters
    /// - `bus_key`: Identity of the bus that owns the subscription.
    /// - `subscription_id`: Bus-local identity to expose during polling and
    ///   future destruction.
    /// - `future`: Operation whose polls and destruction carry that identity.
    ///
    /// # Returns
    /// A future wrapper that carries the subscription identity while polled
    /// and while its wrapped future is destroyed.
    #[must_use = "futures do nothing unless they are polled"]
    pub(in crate::facade) fn new_for_subscription(bus_key: usize, subscription_id: qubit_id::Id, future: F) -> Self {
        Self {
            bus_key,
            subscription_id: Some(subscription_id),
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
        let _scope = enter_bus_scope(this.bus_key, this.subscription_id);
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
            let _scope = enter_bus_scope(self.bus_key, self.subscription_id);
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
fn enter_bus_scope(bus_key: usize, subscription_id: Option<qubit_id::Id>) -> bus_poll_scope::BusPollScope {
    ACTIVE_BUS_POLLS.with(|active| active.borrow_mut().push((bus_key, subscription_id)));
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
    let _scope = enter_bus_scope(bus_key, None);
    action()
}

/// Runs synchronous cleanup with a subscription-level poll identity active.
///
/// # Type Parameters
/// - `R`: Result returned by the cleanup action.
///
/// # Parameters
/// - `bus_key`: Identity of the owning bus.
/// - `subscription_id`: Bus-local identity to expose during cleanup.
/// - `action`: Synchronous cleanup operation to run in that context.
///
/// # Returns
/// The value returned by `action`.
pub(in crate::facade) fn with_subscription_scope<R>(
    bus_key: usize,
    subscription_id: qubit_id::Id,
    action: impl FnOnce() -> R,
) -> R {
    let _scope = enter_bus_scope(bus_key, Some(subscription_id));
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
    ACTIVE_BUS_POLLS.with(|active| active.borrow().iter().any(|frame| frame.0 == bus_key))
}

/// Reports whether the current thread is polling or destroying an operation
/// for this bus-local subscription identity.
///
/// # Parameters
/// - `bus_key`: Facade identity to match.
/// - `subscription_id`: Subscription identity to match within that facade.
///
/// # Returns
/// `true` when a matching subscription frame exists in the active scope stack.
#[must_use = "check whether this subscription is currently being polled"]
#[inline]
pub(in crate::facade) fn is_current_subscription_poll(bus_key: usize, subscription_id: qubit_id::Id) -> bool {
    ACTIVE_BUS_POLLS.with(|active| {
        active
            .borrow()
            .iter()
            .any(|frame| frame.0 == bus_key && frame.1 == Some(subscription_id))
    })
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
    use super::is_current_subscription_poll;
    use super::with_bus_scope;

    struct CheckSubscriptionScope(Arc<std::sync::atomic::AtomicBool>, usize, qubit_id::Id);

    impl Future for CheckSubscriptionScope {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.0.store(
                is_current_subscription_poll(self.1, self.2),
                std::sync::atomic::Ordering::SeqCst,
            );
            assert!(!is_current_subscription_poll(
                self.1,
                qubit_id::Id::new(self.2.value() + 1)
            ));
            assert!(!is_current_subscription_poll(self.1 + 1, self.2));
            with_bus_scope(self.1, || assert!(is_current_subscription_poll(self.1, self.2)));
            Poll::Pending
        }
    }

    impl Drop for CheckSubscriptionScope {
        fn drop(&mut self) {
            self.0.store(
                is_current_subscription_poll(self.1, self.2),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }

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

    #[test]
    fn test_subscription_scope_survives_nested_bus_scope_and_restores() {
        let bus_key = 29;
        let subscription_id = qubit_id::Id::new(7);
        with_bus_scope(bus_key, || {
            assert!(!is_current_subscription_poll(bus_key, subscription_id));
            let observed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let mut future = Box::pin(BusContextFuture::new_for_subscription(
                bus_key,
                subscription_id,
                CheckSubscriptionScope(observed.clone(), bus_key, subscription_id),
            ));
            assert!(
                future
                    .as_mut()
                    .poll(&mut Context::from_waker(std::task::Waker::noop()))
                    .is_pending()
            );
            assert!(observed.load(std::sync::atomic::Ordering::SeqCst));
            with_bus_scope(bus_key, || {
                assert!(!is_current_subscription_poll(bus_key, subscription_id));
                let mut nested = Box::pin(BusContextFuture::new_for_subscription(
                    bus_key,
                    subscription_id,
                    CheckSubscriptionScope(observed.clone(), bus_key, subscription_id),
                ));
                assert!(
                    nested
                        .as_mut()
                        .poll(&mut Context::from_waker(std::task::Waker::noop()))
                        .is_pending()
                );
                assert!(observed.load(std::sync::atomic::Ordering::SeqCst));
                drop(nested);
            });
            drop(future);
            assert!(observed.load(std::sync::atomic::Ordering::SeqCst));
            assert!(!is_current_subscription_poll(bus_key, subscription_id));
        });
        assert!(!is_current_bus_poll(bus_key));
    }
}
