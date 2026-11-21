// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Captures panics raised while polling an asynchronous provider operation.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use super::cleanup_future_guard::CleanupFutureGuard;
use crate::error::SpiError;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::model::ProviderId;
use crate::spi::ReceiveOutcome;
use crate::spi::SpiFuture;
use crate::spi::panic_boundary::provider_panic;

/// Future wrapper that converts polling panics into an output value.
///
/// # Type Parameters
/// - `F`: provider future whose polling is isolated.
pub(in crate::facade::async_event_bus) struct CatchSpiFuture<F: Future> {
    /// Pinned provider future being polled.
    pub(in crate::facade::async_event_bus) future: Pin<Box<F>>,
}

/// Captures a provider panic while polling its future.
///
/// # Type Parameters
/// - `T`: successful provider operation output.
/// - `F`: provider future whose output is preserved.
///
/// # Parameters
/// - `future`: provider operation future to poll behind an unwind boundary.
/// - `provider_id`: identity attached to a captured panic.
/// - `operation`: stable operation name included in the error.
/// - `resource`: optional topic or subscription context.
///
/// # Returns
/// The provider output or a structured panic error.
///
/// # Errors
/// Preserves a provider's returned [`SpiError`] and converts a polling unwind
/// into a non-retryable `provider_panicked` operation error.
pub(in crate::facade) async fn catch_spi_future<T, F: Future<Output = Result<T, SpiError>>>(
    future: F,
    provider_id: &ProviderId,
    operation: &'static str,
    resource: Option<&str>,
) -> Result<T, SpiError> {
    let guarded = CatchSpiFuture {
        future: Box::pin(future),
    };
    guarded
        .await
        .map_err(|payload| provider_panic(provider_id.as_str(), operation, resource, payload))?
}

/// Installs cancellation protection before returning the receive future.
///
/// A destructor panic while receive is cancelled is disposed, preserving the
/// competing scheduler event. Once polling has produced an outcome, a
/// destructor panic is secondary and the original outcome or SPI error wins.
/// Panics while polling are still converted to `provider_panicked`.
///
/// # Parameters
/// - `future`: provider receive future to protect.
/// - `provider_id`: provider identity attached to polling panics.
/// - `resource`: optional subscription context.
///
/// # Returns
/// A future preserving the receive result while isolating destructor panics.
pub(in crate::facade) fn catch_receive_spi_future<'a>(
    future: SpiFuture<'a, Result<ReceiveOutcome, SpiError>>,
    provider_id: &'a ProviderId,
    resource: Option<&'a str>,
) -> impl Future<Output = Result<ReceiveOutcome, SpiError>> + Send + 'a {
    let mut guard = CleanupFutureGuard { future: Some(future) };
    async move {
        let result = catch_spi_future(
            guard.future.as_mut().expect("receive future is present").as_mut(),
            provider_id,
            "receive",
            resource,
        )
        .await;
        match guard.drop_future() {
            Ok(()) => result,
            Err(payload) => {
                dispose_panic_payload(payload);
                result
            }
        }
    }
}

/// Awaits a cleanup future and isolates panics from destroying it, both after
/// completion and when the awaiting operation is cancelled.
///
/// # Type Parameters
/// - `T`: successful cleanup output.
///
/// # Parameters
/// - `future`: pinned provider future retained until its terminal result is
///   known.
/// - `provider_id`: provider identity attached to a destructor panic.
/// - `operation`: stable cleanup operation name.
/// - `resource`: optional subscription or topic context.
///
/// # Returns
/// The provider result, or a structured error for a terminal destructor panic.
/// On cancellation, a destructor panic is disposed and does not replace the
/// caller's timeout, shutdown upgrade, or cancellation behavior.
///
/// # Errors
/// Preserves a provider error over a secondary destructor panic; otherwise
/// converts that panic to `provider_panicked`.
pub(in crate::facade) fn catch_cleanup_spi_future<'a, T: 'a>(
    future: SpiFuture<'a, Result<T, SpiError>>,
    provider_id: &'a ProviderId,
    operation: &'static str,
    resource: Option<&'a str>,
) -> impl Future<Output = Result<T, SpiError>> + Send + 'a {
    let mut guard = CleanupFutureGuard { future: Some(future) };
    async move {
        let result = catch_spi_future(
            guard.future.as_mut().expect("cleanup future is present").as_mut(),
            provider_id,
            operation,
            resource,
        )
        .await;
        match guard.drop_future() {
            Ok(()) => result,
            Err(payload) => match result {
                Err(error) => {
                    dispose_panic_payload(payload);
                    Err(error)
                }
                Ok(_) => Err(provider_panic(provider_id.as_str(), operation, resource, payload)),
            },
        }
    }
}

impl<F: Future> Future for CatchSpiFuture<F> {
    /// Provider output or the captured panic payload.
    type Output = Result<F::Output, Box<dyn Any + Send>>;

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
        match catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::panic::panic_any;
    use std::pin::Pin;
    use std::pin::pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::catch_cleanup_spi_future;
    use super::catch_receive_spi_future;
    use super::catch_spi_future;
    use crate::SpiError;
    use crate::model::ProviderId;
    use crate::spi::ReceiveOutcome;

    struct PanicPayload;
    impl Drop for PanicPayload {
        fn drop(&mut self) {
            panic!("panic payload destructor");
        }
    }

    #[test]
    fn test_async_poll_panic_payload_is_disposed_and_classified() {
        let provider_id = ProviderId::new("async-panic-test").expect("valid provider ID");
        let future = async {
            panic_any(PanicPayload);
            #[allow(unreachable_code)]
            Ok::<(), SpiError>(())
        };
        let guarded = catch_spi_future(future, &provider_id, "publish", Some("topic"));
        let mut guarded = pin!(guarded);
        let mut context = Context::from_waker(Waker::noop());
        let result = match Future::poll(guarded.as_mut(), &mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("panic future should finish in one poll"),
        };
        assert!(matches!(result, Err(SpiError::Operation {
            provider_id, operation: "publish", resource: Some(resource),
            kind: "provider_panicked", retryable: Some(false), ..
        }) if provider_id.as_ref() == "async-panic-test" && resource.as_ref() == "topic"));
    }

    struct DropPanicFuture {
        result: Option<Result<(), SpiError>>,
        drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Future for DropPanicFuture {
        type Output = Result<(), SpiError>;

        fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(self.result.take().expect("polled once"))
        }
    }

    impl Drop for DropPanicFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("cleanup future drop panic");
        }
    }

    #[test]
    fn test_cleanup_future_drop_panic_is_classified_and_spi_error_wins() {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        let provider_id = ProviderId::new("cleanup-drop-test").expect("provider ID");
        let drops = std::sync::Arc::new(AtomicUsize::new(0));
        let success = DropPanicFuture {
            result: Some(Ok(())),
            drops: drops.clone(),
        };
        let result = futures_lite_block_on(catch_cleanup_spi_future(
            Box::pin(success),
            &provider_id,
            "close",
            Some("receiver"),
        ));
        assert!(matches!(
            result,
            Err(SpiError::Operation {
                operation: "close",
                kind: "provider_panicked",
                ..
            })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let original = SpiError::Operation {
            provider_id: "cleanup-drop-test".into(),
            operation: "shutdown",
            resource: None,
            kind: "original_failure",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("original failure")),
        };
        let failed = DropPanicFuture {
            result: Some(Err(original)),
            drops: drops.clone(),
        };
        let result = futures_lite_block_on(catch_cleanup_spi_future(
            Box::pin(failed),
            &provider_id,
            "shutdown",
            None,
        ));
        assert!(matches!(
            result,
            Err(SpiError::Operation {
                operation: "shutdown",
                kind: "original_failure",
                ..
            })
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    struct ReadyReceiveDropPanic {
        drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Future for ReadyReceiveDropPanic {
        type Output = Result<ReceiveOutcome, SpiError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(ReceiveOutcome::Closed))
        }
    }

    impl Drop for ReadyReceiveDropPanic {
        fn drop(&mut self) {
            self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("receive future cleanup panic");
        }
    }

    #[test]
    fn test_receive_future_is_guarded_before_poll_and_preserves_ready_outcome() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        let provider_id = ProviderId::new("receive-drop-test").expect("provider ID");
        let drops = Arc::new(AtomicUsize::new(0));
        let wrapped = catch_receive_spi_future(
            Box::pin(ReadyReceiveDropPanic { drops: drops.clone() }),
            &provider_id,
            Some("receiver"),
        );
        let result = futures_lite_block_on(wrapped).expect("secondary destructor panic is isolated");
        assert!(matches!(result, ReceiveOutcome::Closed));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        let cancelled_drops = Arc::new(AtomicUsize::new(0));
        let wrapped = catch_receive_spi_future(
            Box::pin(PendingReceiveDropPanic(cancelled_drops.clone())),
            &provider_id,
            None,
        );
        let cancellation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(wrapped)));
        assert!(cancellation.is_ok(), "cancellation cleanup panic must not escape Drop");
        assert_eq!(cancelled_drops.load(Ordering::SeqCst), 1);
    }

    struct PendingReceiveDropPanic(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl Future for PendingReceiveDropPanic {
        type Output = Result<ReceiveOutcome, SpiError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingReceiveDropPanic {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            panic!("pending receive future cleanup panic");
        }
    }

    #[test]
    fn test_cleanup_future_is_guarded_before_first_poll() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        let provider_id = ProviderId::new("cleanup-first-poll-test").expect("provider ID");
        let drops = Arc::new(AtomicUsize::new(0));
        let wrapped = catch_cleanup_spi_future(
            Box::pin(DropPanicFuture {
                result: Some(Ok(())),
                drops: drops.clone(),
            }),
            &provider_id,
            "settle",
            None,
        );
        let cancellation = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(wrapped)));
        assert!(cancellation.is_ok(), "unpolled cleanup future drop must be protected");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    fn futures_lite_block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        let mut context = Context::from_waker(std::task::Waker::noop());
        match Future::poll(future.as_mut(), &mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future should complete in one poll"),
        }
    }
}
