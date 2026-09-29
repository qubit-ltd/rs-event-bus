// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Captures panics raised while polling an asynchronous provider operation.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::task::Poll;

use crate::error::SpiError;
use crate::model::ProviderId;

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
    guarded.await.map_err(|payload| {
        crate::spi::panic_boundary::provider_panic(provider_id.as_str(), operation, resource, payload)
    })?
}

impl<F: Future> Future for CatchSpiFuture<F> {
    /// Provider output or the captured panic payload.
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    /// Polls the provider future inside an unwind boundary.
    ///
    /// # Parameters
    /// - `self`: pinned wrapper whose provider future is polled.
    /// - `context`: task context used by the provider future.
    ///
    /// # Returns
    /// Pending, a successful provider output, or a captured panic payload.
    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic)),
        }
    }
}
