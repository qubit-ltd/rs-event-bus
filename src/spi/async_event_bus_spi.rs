//! Runtime-neutral asynchronous event bus backend contract.

use crate::error::SpiError;
use crate::model::PublishAcknowledgement;

use super::AsyncEventSubscriptionSpi;
use super::EventBusCapabilities;
use super::OutboundMessage;
use super::ShutdownMode;
use super::ShutdownOutcome;
use super::SpiFuture;
use super::SpiSubscriptionRequest;

/// Minimal object-safe asynchronous transport interface implemented by a backend.
pub trait AsyncEventBusSpi: Send + Sync + 'static {
    /// Returns the immutable capabilities of this backend instance.
    fn capabilities(&self) -> EventBusCapabilities;

    /// Publishes one transport message.
    fn publish<'a>(
        &'a self,
        message: OutboundMessage,
    ) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>>;

    /// Creates one asynchronous single-owner subscription receiver.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>;

    /// Closes this backend according to the requested mode.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>>;
}
