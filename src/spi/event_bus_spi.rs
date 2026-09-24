// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous event bus backend contract.

use std::time::Duration;

use super::EventBusCapabilities;
use super::EventSubscriptionSpi;
use super::OutboundMessage;
use super::ShutdownMode;
use super::ShutdownOutcome;
use super::SpiSubscriptionRequest;
use super::TopicAddress;
use crate::error::SpiError;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;

/// Minimal object-safe synchronous transport interface implemented by a
/// backend.
///
/// Backend implementations expose immutable capabilities and transport
/// operations; user handlers and event-bus policies stay in the facade.
///
/// # Examples
///
/// A facade can inspect an erased provider without knowing its concrete type:
///
/// ```
/// use qubit_event_bus::spi::EventBusSpi;
///
/// fn payload_modes(provider: &dyn EventBusSpi) -> qubit_event_bus::spi::PayloadModes {
///     provider.capabilities().payload_modes()
/// }
/// ```
///
/// A deliberately rejecting implementation illustrates the minimum object-safe
/// shape. A real provider replaces the two unsupported operations with its
/// transport and subscription receiver:
///
/// ```
/// use qubit_event_bus::error::SpiError;
/// use qubit_event_bus::model::PublishAcknowledgement;
/// use qubit_event_bus::spi::{
///     DelayedDeliveryCapability, DurabilityCapability, EventBusCapabilities,
///     EventBusSpi, EventSubscriptionSpi, OrderingCapability, OutboundMessage,
///     PayloadModes, PublishGuarantee, PublishVisibility, ReplayCapability,
///     SettlementCapabilities, ShutdownMode, ShutdownOutcome, SpiSubscriptionRequest,
/// };
/// use std::sync::Arc;
///
/// struct RejectingExample;
///
/// fn unsupported(operation: &'static str, resource: Option<&str>) -> SpiError {
///     SpiError::Operation {
///         provider_id: "example".into(),
///         operation,
///         resource: resource.map(Into::into),
///         kind: "unsupported",
///         retryable: Some(false),
///         source: Box::new(std::io::Error::other("example SPI has no transport")),
///     }
/// }
///
/// impl EventBusSpi for RejectingExample {
///     fn capabilities(&self) -> EventBusCapabilities {
///         EventBusCapabilities::new(
///             PayloadModes::Native,
///             SettlementCapabilities::None,
///             OrderingCapability::None,
///             DelayedDeliveryCapability::None,
///             DurabilityCapability::Ephemeral,
///             false,
///             ReplayCapability::None,
///             PublishGuarantee::FireAndForget,
///             PublishVisibility::Opaque,
///         )
///     }
///
///     fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
///         Err(unsupported("publish", Some(message.topic().as_str())))
///     }
///
///     fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
///         Err(unsupported("subscribe", Some(request.topic().as_str())))
///     }
///
///     fn shutdown(&self, _mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
///         Ok(ShutdownOutcome::Complete)
///     }
/// }
///
/// let spi: Arc<dyn EventBusSpi> = Arc::new(RejectingExample);
/// assert_eq!(spi.capabilities().payload_modes(), PayloadModes::Native);
/// ```
pub trait EventBusSpi: Send + Sync + 'static {
    /// Returns an optional facade provider identity attached by a registry.
    ///
    /// Provider implementations should leave the default unchanged. Registry
    /// adapters override this hidden metadata hook on a transparent proxy so
    /// the facade can report the exact provider that succeeded after fallback.
    #[doc(hidden)]
    fn provider_id(&self) -> Option<ProviderId> {
        None
    }

    /// Returns the immutable capabilities of this backend instance.
    fn capabilities(&self) -> EventBusCapabilities;

    /// Publishes one type-erased transport message.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError>;

    /// Creates one single-owner subscription receiver.
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;

    /// Waits until this provider has no outstanding delivery for `topic`.
    ///
    /// Returns `Ok(None)` when the provider cannot make this guarantee. A
    /// returned `true` means all queued and unsettled deliveries are gone;
    /// `false` means the timeout elapsed first. This does not report whether a
    /// handler succeeded.
    fn wait_for_topic_idle(&self, _topic: &TopicAddress, _timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        Ok(None)
    }

    /// Closes this backend according to the requested shutdown mode.
    ///
    /// The facade may call this method again after a failed shutdown attempt.
    /// Repeated calls must safely continue or finish shutdown; calling it after
    /// the backend has already closed must succeed and report a stable outcome.
    /// A later `Immediate` call may strengthen a previously requested
    /// `Graceful` shutdown.
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError>;
}
