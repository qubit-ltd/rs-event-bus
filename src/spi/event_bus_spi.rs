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
/// use qubit_event_bus::spi::PayloadModes;
///
/// fn payload_modes(provider: &dyn EventBusSpi) -> PayloadModes {
///     provider.capabilities().payload_modes()
/// }
/// ```
///
/// A deliberately rejecting implementation illustrates the minimum object-safe
/// shape. A real provider replaces the two unsupported operations with its
/// transport and subscription receiver:
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_event_bus::error::SpiError;
/// use qubit_event_bus::model::PublishAcknowledgement;
/// use qubit_event_bus::spi::DelayedDeliveryCapability;
/// use qubit_event_bus::spi::DurabilityCapability;
/// use qubit_event_bus::spi::EventBusCapabilities;
/// use qubit_event_bus::spi::EventBusSpi;
/// use qubit_event_bus::spi::EventSubscriptionSpi;
/// use qubit_event_bus::spi::OrderingCapability;
/// use qubit_event_bus::spi::OutboundMessage;
/// use qubit_event_bus::spi::PayloadModes;
/// use qubit_event_bus::spi::PublishGuarantee;
/// use qubit_event_bus::spi::PublishVisibility;
/// use qubit_event_bus::spi::ReplayCapability;
/// use qubit_event_bus::spi::SettlementCapabilities;
/// use qubit_event_bus::spi::ShutdownMode;
/// use qubit_event_bus::spi::ShutdownOutcome;
/// use qubit_event_bus::spi::SpiSubscriptionRequest;
/// use qubit_event_bus::spi::SubscriptionModes;
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
///         EventBusCapabilities::builder()
///             .payload_modes(PayloadModes::Native)
///             .settlement(SettlementCapabilities::None)
///             .ordering(OrderingCapability::None)
///             .delayed_delivery(DelayedDeliveryCapability::None)
///             .durability(DurabilityCapability::Ephemeral)
///             .subscription_modes(SubscriptionModes::EPHEMERAL)
///             .consumer_groups(false)
///             .replay(ReplayCapability::None)
///             .publish_guarantee(PublishGuarantee::FireAndForget)
///             .publish_visibility(PublishVisibility::Opaque)
///             .build()
///             .expect("all example capabilities are configured")
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
    ///
    /// # Returns
    /// The attached provider identity, or `None` when no registry identity is
    /// attached.
    #[doc(hidden)]
    #[inline]
    fn provider_id(&self) -> Option<ProviderId> {
        None
    }

    /// Returns the immutable capabilities of this backend instance.
    ///
    /// Implementations must keep the returned value stable for the lifetime
    /// of the SPI instance; a facade snapshots it during construction.
    ///
    /// # Returns
    /// The immutable capabilities supported by this provider instance.
    #[must_use = "Use the returned query result."]
    fn capabilities(&self) -> EventBusCapabilities;

    /// Publishes one type-erased transport message.
    ///
    /// # Parameters
    /// - `message`: validated outbound message to send.
    ///
    /// # Returns
    /// The provider's admission acknowledgement.
    ///
    /// # Errors
    /// Returns a structured provider operation failure.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError>;

    /// Creates one single-owner subscription receiver.
    ///
    /// # Parameters
    /// - `request`: provider subscription identity, topic, and policies.
    ///
    /// # Returns
    /// A receiver owned by the caller.
    ///
    /// # Errors
    /// Returns a structured provider operation failure.
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError>;

    /// Waits until this provider has no outstanding delivery for `topic`.
    ///
    /// Returns `Ok(None)` when the provider cannot make this guarantee. A
    /// returned `true` means all queued and unsettled deliveries are gone;
    /// `false` means the timeout elapsed first. This does not report whether a
    /// handler succeeded.
    ///
    /// # Parameters
    /// - `topic`: provider destination whose outstanding deliveries are
    ///   checked.
    /// - `timeout`: maximum wait, or `None` to wait without a deadline.
    ///
    /// # Returns
    /// `Some(true)` when idle, `Some(false)` on timeout, or `None` when the
    /// provider cannot make this guarantee.
    ///
    /// # Errors
    /// Returns a structured provider operation failure.
    #[inline]
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
    ///
    /// # Parameters
    /// - `mode`: requested graceful or immediate shutdown behavior.
    ///
    /// # Returns
    /// The provider shutdown outcome.
    ///
    /// # Errors
    /// Returns a structured provider operation failure.
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError>;
}
