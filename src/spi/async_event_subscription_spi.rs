// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous subscription contract.

use std::time::Duration;

use super::DeliveryDisposition;
use super::ReceiveOutcome;
use super::SettlementToken;
use super::SpiFuture;
use crate::error::SpiError;

/// An asynchronously consumed receiver owned by a facade or subscription
/// runner.
///
/// Implementations must release or safely detach provider resources when this
/// receiver is dropped. Before a runner starts, an asynchronous facade may
/// retain the receiver so it can await [`Self::close`] during bus shutdown;
/// after ownership transfers to the runner, the runner closes it on completion.
/// Facades cannot await cleanup from `Drop`, so implementations must still
/// safely release or detach resources if the receiver is dropped without close.
/// Any delivery whose [`SettlementToken`] has not reached a terminal
/// disposition must remain recoverable by the provider after receiver close or
/// drop (for example, by broker redelivery or returning it to a local queue).
/// Receiver cleanup must never implicitly acknowledge an unsettled delivery.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
///
/// fn supports_async_receive(receiver: &mut dyn AsyncEventSubscriptionSpi) {
///     let future = receiver.receive(std::time::Duration::ZERO);
///     drop(future); // The SPI contract keeps a message available after cancellation.
/// }
/// ```
pub trait AsyncEventSubscriptionSpi: Send + 'static {
    /// Receives one outcome without losing a message if the returned future is
    /// cancelled.
    ///
    /// If cancellation happens after a message has been consumed from the
    /// provider, that message must remain buffered for or be redelivered to the
    /// next `receive` call. A provider whose receive operation is not
    /// inherently cancellation-safe must continuously consume in an
    /// internal task and buffer messages, placing the cancellable boundary
    /// at the buffer read.
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>>;

    /// Applies a terminal disposition to a provider-issued token.
    ///
    /// Repeating the same token and disposition must be idempotent and return
    /// the same terminal result. Reusing the token with a different disposition
    /// must return a structured invalid-token error. If this future is
    /// cancelled after the provider may have applied the disposition, callers
    /// may retry with the same borrowed token and disposition; providers must
    /// make both in-progress and completed settlement attempts idempotent.
    /// Providers must synchronously derive any owned operation state before
    /// returning the future; the future must not borrow the token.
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>>;

    /// Closes this receiver and releases its resources.
    ///
    /// Unsettled deliveries must remain recoverable by the provider after
    /// close; closing is not an implicit acknowledgement. Dropping the
    /// receiver without calling this method has the same no-loss requirement.
    /// Dropping this future does not guarantee that provider-side close work
    /// was rolled back. Calling `close` again on the same receiver must be
    /// safe and converge to the closed state; closing an already-closed
    /// receiver must succeed. This lets a facade retry after timeout or future
    /// cancellation without losing ownership of the receiver.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>>;
}
