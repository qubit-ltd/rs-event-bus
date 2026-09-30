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

/// Future adapter that returns panic payloads instead of unwinding its caller.
///
/// # Type Parameters
/// - `F`: Provider future whose polling is isolated.
#[must_use]
pub(super) struct CatchUnwindFuture<F: Future> {
    /// Pinned SPI operation future being polled.
    future: Pin<Box<F>>,
}

impl<F: Future> CatchUnwindFuture<F> {
    /// Pins the SPI future for panic-isolated polling.
    ///
    /// # Parameters
    /// - `future`: provider operation whose polling is isolated.
    ///
    /// # Returns
    /// A wrapper that captures panics from future polling.
    pub(super) fn new(future: F) -> Self {
        Self {
            future: Box::pin(future),
        }
    }
}

impl<F: Future> Future for CatchUnwindFuture<F> {
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
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}
