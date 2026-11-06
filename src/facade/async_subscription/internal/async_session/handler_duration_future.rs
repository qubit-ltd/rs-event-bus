// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async adapter retaining handler duration through future cleanup.

use std::future::Future;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use crate::error::DeliveryError;
use crate::facade::async_subscription::internal::handler_duration_guard::HandlerDurationGuard;
use crate::spi::SpiFuture;

/// Handler future paired with the guard that records its execution duration.
pub(super) struct HandlerDurationFuture {
    /// User handler future retained until the outer panic boundary drops it.
    future: SpiFuture<'static, Result<(), DeliveryError>>,
    /// Records duration after the handler future has been destroyed.
    _duration: Option<HandlerDurationGuard>,
}

impl HandlerDurationFuture {
    /// Creates an adapter that keeps the duration guard alive with the future.
    ///
    /// # Parameters
    /// - `future`: user handler future to poll.
    /// - `duration`: guard recording the full handler execution interval.
    ///
    /// # Returns
    /// An adapter whose fields are destroyed by the surrounding panic boundary.
    pub(super) fn new(future: SpiFuture<'static, Result<(), DeliveryError>>, duration: HandlerDurationGuard) -> Self {
        Self {
            future,
            _duration: Some(duration),
        }
    }
}

impl Future for HandlerDurationFuture {
    /// The handler result after one poll completes.
    type Output = Result<(), DeliveryError>;

    /// Polls the handler future and ends duration timing before ready cleanup.
    ///
    /// # Parameters
    /// - `self`: pinned adapter retaining the handler future.
    /// - `context`: task context used by the handler future.
    ///
    /// # Returns
    /// The handler future's pending state or output.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match this.future.as_mut().poll(context) {
            Poll::Ready(result) => {
                drop(this._duration.take());
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
