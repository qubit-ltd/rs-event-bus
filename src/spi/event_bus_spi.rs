//! Synchronous event bus backend contract.

use crate::error::SpiError;
use crate::model::PublishAcknowledgement;

use super::EventBusCapabilities;
use super::EventSubscriptionSpi;
use super::OutboundMessage;
use super::ShutdownMode;
use super::ShutdownOutcome;
use super::SpiSubscriptionRequest;

/// Minimal object-safe synchronous transport interface implemented by a backend.
pub trait EventBusSpi: Send + Sync + 'static {
    /// Returns the immutable capabilities of this backend instance.
    fn capabilities(&self) -> EventBusCapabilities;

    /// Publishes one type-erased transport message.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError>;

    /// Creates one single-owner subscription receiver.
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;

    /// Closes this backend according to the requested shutdown mode.
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError>;
}
