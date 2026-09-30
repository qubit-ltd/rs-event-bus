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

/// Future wrapper that isolates subscriber middleware and handler panics.
///
/// # Type Parameters
/// - `F`: delivery future whose polling is isolated.
#[must_use]
pub(in crate::pipeline::subscriber_pipeline) struct CatchUnwindFuture<F: Future> {
    /// Pinned middleware or handler future being polled.
    pub(in crate::pipeline::subscriber_pipeline) future: Pin<Box<F>>,
}

impl<F: Future<Output = Result<(), DeliveryError>>> Future for CatchUnwindFuture<F> {
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
        match catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            Ok(Poll::Ready(Err(error))) => Poll::Ready(Err(error)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(DeliveryError::Handler {
                source: Box::new(Error::other(panic_message(panic.as_ref()))),
            })),
        }
    }
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
