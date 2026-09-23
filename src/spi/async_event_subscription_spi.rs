//! Runtime-neutral asynchronous subscription contract.

use std::time::Duration;

use crate::error::SpiError;

use super::DeliveryDisposition;
use super::ReceiveOutcome;
use super::SettlementToken;
use super::SpiFuture;

/// An asynchronously consumed receiver owned by one caller.
pub trait AsyncEventSubscriptionSpi: Send + 'static {
    /// Receives one outcome without losing a message if the returned future is cancelled.
    ///
    /// If cancellation happens after a message has been consumed from the
    /// provider, that message must remain buffered for or be redelivered to the
    /// next `receive` call. A provider whose receive operation is not inherently
    /// cancellation-safe must continuously consume in an internal task and
    /// buffer messages, placing the cancellable boundary at the buffer read.
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>>;

    /// Applies a terminal disposition to a provider-issued token.
    fn settle<'a>(
        &'a mut self,
        token: SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>>;

    /// Closes this receiver and releases its resources.
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>>;
}
