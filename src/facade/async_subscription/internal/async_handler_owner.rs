// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared ownership for asynchronous subscriber callbacks.

use std::ops::Deref;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::Diagnostic;
use crate::error::DeliveryError;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::async_subscription::with_subscription_scope;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::model::Delivery;
use crate::spi::SpiFuture;

/// Boxed subscriber handler that returns a runtime-neutral future.
/// The callback can be shared by delivery tasks without tying the session to
/// a specific executor's future type.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
type AsyncHandler<T> = dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync;

/// Shared owner of a subscriber handler callback.
/// Clones retain the same callback for concurrent delivery tasks.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
pub(in crate::facade::async_subscription) type SharedAsyncHandler<T> = Arc<AsyncHandlerOwner<T>>;

/// Shared callback used only by an internal delivery wrapper.
pub(in crate::facade::async_subscription) type SharedAsyncCallback<T> = Arc<AsyncHandler<T>>;

/// Shares an internal callback in a single allocation.
pub(in crate::facade::async_subscription) fn share_async_callback<T, F>(callback: F) -> SharedAsyncCallback<T>
where
    F: Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static,
{
    Arc::new(callback)
}

/// Shared callback owner that contains destruction only after replacement.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
pub(in crate::facade::async_subscription) struct AsyncHandlerOwner<T: 'static> {
    /// Callback retained for the lifetime of this owner.
    callback: Option<Box<AsyncHandler<T>>>,
    /// Whether replacement transferred final-drop handling to this owner.
    replaced: AtomicBool,
    /// Bus used to scope destruction and report a panic.
    inner: Weak<AsyncEventBusInner>,
    /// Subscription context associated with the callback.
    subscription_id: qubit_id::Id,
}

/// Operations for shared asynchronous handler owners.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
impl<T: 'static> AsyncHandlerOwner<T> {
    /// Creates a shared owner for a subscriber callback.
    ///
    /// # Type Parameters
    /// - `T`: payload type accepted by the handler.
    /// - `F`: callback type implementing the handler contract.
    ///
    /// # Parameters
    /// - `callback`: subscriber callback to retain.
    /// - `inner`: bus that owns the subscription.
    /// - `subscription_id`: ID used when scoping destruction diagnostics.
    ///
    /// # Returns
    /// A shared owner that can be cloned by delivery tasks.
    pub(in crate::facade::async_subscription) fn new<F>(
        callback: F,
        inner: Arc<AsyncEventBusInner>,
        subscription_id: qubit_id::Id,
    ) -> SharedAsyncHandler<T>
    where
        F: Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static,
    {
        Arc::new(Self {
            callback: Some(Box::new(callback)),
            replaced: AtomicBool::new(false),
            inner: Arc::downgrade(&inner),
            subscription_id,
        })
    }

    /// Marks the callback as replaced so its final drop is isolated.
    ///
    /// # Parameters
    /// - `self`: owner whose callback was replaced.
    pub(in crate::facade::async_subscription) fn mark_replaced(&self) {
        self.replaced.store(true, Ordering::Release);
    }
}

/// Exposes the retained callback through the shared owner.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
impl<T: 'static> Deref for AsyncHandlerOwner<T> {
    /// Callback type invoked for each delivery.
    type Target = AsyncHandler<T>;

    /// Borrows the retained subscriber callback.
    ///
    /// # Parameters
    /// - `self`: shared callback owner.
    ///
    /// # Returns
    /// The callback retained by this owner.
    fn deref(&self) -> &Self::Target {
        self.callback
            .as_deref()
            .expect("handler callback retained until owner drop")
    }
}

/// Releases the callback according to its replacement state.
///
/// # Type Parameters
/// - `T`: payload type accepted by the handler.
impl<T: 'static> Drop for AsyncHandlerOwner<T> {
    /// Drops the callback and isolates destruction after replacement.
    ///
    /// # Parameters
    /// - `self`: owner whose callback is being released.
    fn drop(&mut self) {
        let Some(callback) = self.callback.take() else {
            return;
        };
        if !self.replaced.load(Ordering::Acquire) {
            drop(callback);
            return;
        }
        let Some(inner) = self.inner.upgrade() else {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(callback))) {
                dispose_panic_payload(payload);
            }
            return;
        };
        let bus_key = Arc::as_ptr(&inner) as usize;
        with_subscription_scope(bus_key, self.subscription_id, || {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(callback))) {
                dispose_panic_payload(payload);
                inner.emit(&Diagnostic::InternalFailure {
                    origin: "replaced_async_handler_drop".into(),
                    message: "replaced async handler drop panicked".into(),
                });
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::DeliveryError;
    use crate::facade::async_subscription::internal::async_handler_owner::share_async_callback;
    use crate::pipeline::subscriber_pipeline::tests::start_allocation_count;
    use crate::pipeline::subscriber_pipeline::tests::stop_allocation_count;
    use crate::spi::SpiFuture;

    #[test]
    fn test_duration_wrapper_callback_uses_one_allocation() {
        let capture = Arc::new(());
        start_allocation_count();
        let wrapper = share_async_callback::<(), _>(move |_| {
            let _keep_owner = &capture;
            Box::pin(async { Ok(()) }) as SpiFuture<'static, Result<(), DeliveryError>>
        });
        let allocations = stop_allocation_count();
        drop(wrapper);
        assert_eq!(allocations, 1);
    }

    #[test]
    fn test_start_gate_wrapper_callback_uses_one_allocation() {
        let capture = Arc::new(());
        start_allocation_count();
        let wrapper = share_async_callback::<(), _>(move |_| {
            let _keep_owner = &capture;
            Box::pin(async { Ok(()) }) as SpiFuture<'static, Result<(), DeliveryError>>
        });
        let allocations = stop_allocation_count();
        drop(wrapper);
        assert_eq!(allocations, 1);
    }
}
