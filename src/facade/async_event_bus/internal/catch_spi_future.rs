// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async facade lifecycle owner.

use crate::facade::async_event_bus::AssertUnwindSafe;
use crate::facade::async_event_bus::Pin;
use crate::model::ProviderId;

/// Converts a panic while polling a provider future into an SPI error.
///
/// # Type Parameters
/// - `F`: provider future type.
/// - `T`: provider result value.
///
/// # Parameters
/// - `future`: provider operation future to poll.
/// - `provider`: provider identity included in a panic report.
/// - `operation`: SPI operation name included in a panic report.
/// - `resource`: optional resource identity included in a panic report.
///
/// # Returns
/// The provider result or a normalized panic error.
///
/// # Errors
/// Returns the provider error or an SPI error describing a panic during poll.
pub(in crate::facade) async fn catch_spi_future<F, T>(
    future: F,
    provider: &ProviderId,
    operation: &'static str,
    resource: Option<&str>,
) -> Result<T, crate::error::SpiError>
where
    F: Future<Output = Result<T, crate::error::SpiError>> + Send,
{
    match (CatchSpiFuture {
        future: Box::pin(future),
    })
    .await
    {
        Ok(result) => result,
        Err(panic) => Err(crate::spi::panic_boundary::provider_panic(
            provider.as_str(),
            operation,
            resource,
            panic,
        )),
    }
}

/// Catches unwinding from each poll of a provider future.
///
/// # Type Parameters
/// - `F`: future whose polling panics are captured.
pub(in crate::facade) struct CatchSpiFuture<F: Future> {
    /// Pinned provider future being polled.
    future: Pin<Box<F>>,
}
impl<F: Future> Future for CatchSpiFuture<F> {
    /// Future output or the captured panic payload.
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;
    /// Polls the provider future inside an unwind boundary.
    ///
    /// # Parameters
    /// - `self`: pinned wrapper whose provider future is polled.
    /// - `context`: task context used by the provider future.
    ///
    /// # Returns
    /// `Pending`, a successful provider output, or a captured panic payload.
    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    }
}
