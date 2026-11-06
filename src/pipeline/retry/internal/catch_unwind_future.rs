// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Catches panics raised while polling an SPI future.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::error::SpiError;
use crate::internal::panic_payload::dispose_panic_payload;

/// Future adapter that returns panic payloads instead of unwinding its caller.
///
/// # Type Parameters
/// - `F`: Provider future whose polling is isolated.
#[must_use]
pub(in crate::pipeline::retry) struct CatchUnwindFuture<F: Future> {
    /// Pinned SPI operation future being polled.
    future: Option<Pin<Box<F>>>,
}

impl<F: Future> CatchUnwindFuture<F> {
    /// Pins the SPI future for panic-isolated polling.
    ///
    /// # Parameters
    /// - `future`: provider operation whose polling is isolated.
    ///
    /// # Returns
    /// A wrapper that captures panics from future polling.
    pub(in crate::pipeline::retry) fn new(future: F) -> Self {
        Self {
            future: Some(Box::pin(future)),
        }
    }
}

impl<F, T> Future for CatchUnwindFuture<F>
where
    F: Future<Output = Result<T, SpiError>>,
{
    /// Provider output or the captured panic payload.
    type Output = Result<Result<T, SpiError>, Box<dyn Any + Send>>;

    /// Polls the provider future inside an unwind boundary.
    ///
    /// # Parameters
    /// - `self`: pinned wrapper whose provider future is polled.
    /// - `context`: task context used by the provider future.
    ///
    /// # Returns
    /// Pending, a successful provider output, or a captured panic payload.
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
            Ok(Poll::Ready(value)) => match (value, drop_future(&mut this.future)) {
                (Err(error), Ok(())) => Poll::Ready(Ok(Err(error))),
                (Err(error), Err(payload)) => {
                    dispose_panic_payload(payload);
                    Poll::Ready(Ok(Err(error)))
                }
                (Ok(value), Ok(())) => Poll::Ready(Ok(Ok(value))),
                (Ok(_), Err(payload)) => {
                    dispose_panic_payload(payload);
                    Poll::Ready(Err(Box::new("provider future cleanup panicked")))
                }
            },
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                if let Err(cleanup_panic) = drop_future(&mut this.future) {
                    dispose_panic_payload(cleanup_panic);
                }
                Poll::Ready(Err(payload))
            }
        }
    }
}

impl<F: Future> Drop for CatchUnwindFuture<F> {
    /// Safely destroys any provider future left during cancellation.
    fn drop(&mut self) {
        if let Err(payload) = drop_future(&mut self.future) {
            dispose_panic_payload(payload);
        }
    }
}

/// Removes and safely destroys a provider future once.
///
/// # Type Parameters
/// - `F`: Provider future whose destruction is protected.
///
/// # Parameters
/// - `future`: optional pinned future owned by the panic adapter.
///
/// # Returns
/// The caught panic payload when the future's destructor panics.
fn drop_future<F: Future>(future: &mut Option<Pin<Box<F>>>) -> Result<(), Box<dyn Any + Send>> {
    match future.take() {
        Some(future) => catch_unwind(AssertUnwindSafe(|| drop(future))),
        None => Ok(()),
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
    use super::dispose_panic_payload;
    use crate::error::SpiError;

    struct ReadyDropPanic(Arc<AtomicUsize>);
    impl Future for ReadyDropPanic {
        type Output = Result<(), SpiError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Err(SpiError::Operation {
                provider_id: "test".into(),
                operation: "publish",
                resource: None,
                kind: "business_error",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("provider error")),
            }))
        }
    }
    impl Drop for ReadyDropPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("SPI future drop panic");
        }
    }

    #[test]
    fn test_future_drop_panic_ready_provider_error_is_preserved() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut wrapped = Box::pin(CatchUnwindFuture::new(ReadyDropPanic(drops.clone())));
        assert!(matches!(
            wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Ok(Err(SpiError::Operation {
                kind: "business_error",
                ..
            })))
        ));
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_future_drop_panic_poll_panic_then_cleanup_is_separate() {
        struct PollAndDropPanic(Arc<AtomicUsize>);
        impl Future for PollAndDropPanic {
            type Output = Result<(), SpiError>;
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                panic!("poll panic");
            }
        }
        impl Drop for PollAndDropPanic {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("drop panic");
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let mut wrapped = Box::pin(CatchUnwindFuture::new(PollAndDropPanic(drops.clone())));
        assert!(matches!(
            wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(_))
        ));
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_future_drop_panic_captured_payload_drop_panic_is_disposed() {
        struct PanicOnDrop(Arc<AtomicUsize>);
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
                panic!("panic payload destructor panic");
            }
        }
        struct PollPanic(Arc<AtomicUsize>);
        impl Future for PollPanic {
            type Output = Result<(), SpiError>;
            fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                std::panic::panic_any(PanicOnDrop(self.0.clone()));
            }
        }
        let drops = Arc::new(AtomicUsize::new(0));
        let mut wrapped = Box::pin(CatchUnwindFuture::new(PollPanic(drops.clone())));
        let Poll::Ready(Err(payload)) = wrapped.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
            panic!("poll panic payload must be captured");
        };
        dispose_panic_payload(payload);
        drop(wrapped);
        assert_eq!(drops.load(Ordering::SeqCst), 1, "panic payload destructor runs once");
    }
}
