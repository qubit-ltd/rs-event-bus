// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Converts panics from subscriber future polling to delivery errors.

use std::any::Any;
use std::future::Future;
use std::io::Error;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::error::DeliveryError;
use crate::internal::panic_payload::dispose_panic_payload;

/// Future wrapper that isolates subscriber middleware and handler panics.
///
/// # Type Parameters
/// - `F`: delivery future whose polling is isolated.
#[must_use]
pub(in crate::pipeline::subscriber_pipeline) struct CatchUnwindFuture<F: Future + ?Sized> {
    /// Pinned middleware or handler future being polled.
    pub(in crate::pipeline::subscriber_pipeline) future: Option<Pin<Box<F>>>,
}

impl<F: Future<Output = Result<(), DeliveryError>> + ?Sized> Future for CatchUnwindFuture<F> {
    /// Result returned by the delivery pipeline.
    type Output = Result<(), DeliveryError>;

    /// Polls the delivery future inside an unwind boundary.
    ///
    /// # Parameters
    /// - `self`: pinned wrapper whose delivery future is polled.
    /// - `context`: task context used by the future.
    ///
    /// # Returns
    /// Pending or the handler result, with panics converted to handler errors.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("future is present until completion")
                .as_mut()
                .poll(context)
        }));
        match polled {
            Ok(Poll::Ready(result)) => {
                let cleanup = drop_future(&mut this.future);
                match (result, cleanup) {
                    (Err(error), cleanup) => {
                        if let Err(payload) = cleanup {
                            dispose_panic_payload(payload);
                        }
                        Poll::Ready(Err(error))
                    }
                    (Ok(()), Ok(())) => Poll::Ready(Ok(())),
                    (Ok(()), Err(payload)) => Poll::Ready(Err(cleanup_panic_error(payload))),
                }
            }
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => {
                if let Err(payload) = drop_future(&mut this.future) {
                    dispose_panic_payload(payload);
                }
                let error = DeliveryError::Handler {
                    source: Box::new(Error::other(panic_message(panic.as_ref()))),
                };
                dispose_panic_payload(panic);
                Poll::Ready(Err(error))
            }
        }
    }
}

impl<F: Future + ?Sized> Drop for CatchUnwindFuture<F> {
    /// Safely destroys any subscriber future left during cancellation.
    fn drop(&mut self) {
        if let Err(payload) = drop_future(&mut self.future) {
            dispose_panic_payload(payload);
        }
    }
}

/// Removes and safely destroys a subscriber future once.
///
/// # Type Parameters
/// - `F`: Subscriber future whose destruction is protected.
///
/// # Parameters
/// - `future`: optional pinned future owned by the panic adapter.
///
/// # Returns
/// The caught panic payload when the future's destructor panics.
fn drop_future<F: Future + ?Sized>(future: &mut Option<Pin<Box<F>>>) -> Result<(), Box<dyn Any + Send>> {
    match future.take() {
        Some(future) => catch_unwind(AssertUnwindSafe(|| drop(future))),
        None => Ok(()),
    }
}

/// Builds a handler error for a successful future with failed cleanup.
///
/// # Parameters
/// - `payload`: panic payload produced by the future destructor.
///
/// # Returns
/// A handler error identifying the cleanup failure.
fn cleanup_panic_error(payload: Box<dyn Any + Send>) -> DeliveryError {
    let error = DeliveryError::Handler {
        source: Box::new(Error::other("subscriber middleware or handler future cleanup panicked")),
    };
    dispose_panic_payload(payload);
    error
}

/// Returns a stable description of a subscriber panic payload.
///
/// # Parameters
/// - `panic`: captured payload returned by `catch_unwind`.
///
/// # Returns
/// A static message that does not expose an arbitrary panic object.
fn panic_message(panic: &(dyn Any + Send)) -> &'static str {
    if panic.is::<&'static str>() || panic.is::<String>() {
        "subscriber middleware or handler panicked"
    } else {
        "subscriber middleware or handler panicked with a non-string payload"
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::CatchUnwindFuture;
    use crate::error::DeliveryError;

    struct DropProbe {
        result: Option<Result<(), DeliveryError>>,
        drops: Arc<AtomicUsize>,
        panic_on_drop: bool,
    }

    impl Future for DropProbe {
        type Output = Result<(), DeliveryError>;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.result.take().expect("polled once"))
        }
    }

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            assert!(!self.panic_on_drop, "probe drop panic");
        }
    }

    #[test]
    fn test_future_drop_panic_ready_cleanup_becomes_handler_error() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = DropProbe {
            result: Some(Ok(())),
            drops: drops.clone(),
            panic_on_drop: true,
        };
        let mut wrapped = Box::pin(CatchUnwindFuture {
            future: Some(Box::pin(future)),
        });
        let result = wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        let Poll::Ready(Err(error)) = result else {
            panic!("cleanup panic must become a handler error");
        };
        assert!(error.to_string().contains("handler"));
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_future_drop_panic_preserves_handler_error() {
        let drops = Arc::new(AtomicUsize::new(0));
        let future = DropProbe {
            result: Some(Err(DeliveryError::Handler {
                source: Box::new(std::io::Error::other("original handler error")),
            })),
            drops: drops.clone(),
            panic_on_drop: true,
        };
        let mut wrapped = Box::pin(CatchUnwindFuture {
            future: Some(Box::pin(future)),
        });
        let Poll::Ready(Err(error)) = wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
            panic!("original handler error must remain the result");
        };
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .to_string()
                .contains("original handler error")
        );
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_future_drop_panic_pending_cancellation_is_suppressed_once() {
        struct PendingDrop(Arc<AtomicUsize>);
        impl Future for PendingDrop {
            type Output = Result<(), DeliveryError>;
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }
        impl Drop for PendingDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("cancel drop panic");
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let mut wrapped = Box::pin(CatchUnwindFuture {
            future: Some(Box::pin(PendingDrop(drops.clone()))),
        });
        assert!(matches!(
            wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
