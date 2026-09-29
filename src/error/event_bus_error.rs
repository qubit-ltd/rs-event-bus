// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate error for event bus operations.

use crate::error::CapabilityError;
use crate::error::CodecError;
use crate::error::ConfigurationError;
use crate::error::DeliveryError;
use crate::error::LifecycleError;
use crate::error::ProviderError;
use crate::error::PublishError;
use crate::error::PublishFailure;
use crate::error::ReceiveError;
use crate::error::SettlementError;
use crate::error::SubscribeError;

/// A typed error from any event bus operation.
///
/// Public facade publication failures convert into [`Self::PublishFailure`],
/// preserving event identity, aggregate admission evidence and the cause.
/// [`Self::Publish`] retains the lower-level cause used inside the publisher
/// pipeline before those publication-level details are attached.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::ConfigurationError;
/// use qubit_event_bus::error::EventBusError;
///
/// let error = EventBusError::Configuration(ConfigurationError::MissingField { field: "topic" });
/// assert!(matches!(error, EventBusError::Configuration(_)));
/// ```
///
/// A facade publication can use `?` in an aggregate result without discarding
/// its structured failure:
///
/// ```
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::EventBusError;
/// use qubit_event_bus::model::PublishReceipt;
/// use qubit_event_bus::model::PublishRequest;
///
/// fn publish(bus: &EventBus, request: PublishRequest<String>)
///     -> Result<PublishReceipt, EventBusError>
/// {
///     Ok(bus.publish(request)?)
/// }
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum EventBusError {
    /// Invalid caller configuration.
    #[error(transparent)]
    Configuration(
        /// Validation failure in a caller-supplied setting.
        #[from]
        ConfigurationError,
    ),
    /// Required provider capability is missing.
    #[error(transparent)]
    Capability(
        /// Capability the selected provider does not support.
        #[from]
        CapabilityError,
    ),
    /// A payload codec failed.
    #[error(transparent)]
    Codec(
        /// Failure while encoding or decoding an event payload.
        #[from]
        CodecError,
    ),
    /// A lower-level publishing cause, before publication identity and effect
    /// are attached by the facade.
    #[error(transparent)]
    Publish(
        /// Cause retained by the internal publisher pipeline.
        #[from]
        PublishError,
    ),
    /// A public facade publication failed, retaining event identity, aggregate
    /// admission evidence and the original cause chain.
    #[error(transparent)]
    PublishFailure(
        /// Complete failure returned by synchronous or asynchronous
        /// publication.
        #[from]
        PublishFailure,
    ),
    /// Subscription creation failed.
    #[error(transparent)]
    Subscribe(
        /// Failure reported while creating a subscription.
        #[from]
        SubscribeError,
    ),
    /// Receiving failed.
    #[error(transparent)]
    Receive(
        /// Failure reported while receiving a delivery.
        #[from]
        ReceiveError,
    ),
    /// Delivery processing failed.
    #[error(transparent)]
    Delivery(
        /// Handler, codec, or delivery processing failure.
        #[from]
        DeliveryError,
    ),
    /// Delivery settlement failed.
    #[error(transparent)]
    Settlement(
        /// Failure while applying an accept, retry, or reject decision.
        #[from]
        SettlementError,
    ),
    /// A lifecycle operation failed.
    #[error(transparent)]
    Lifecycle(
        /// Failure while waiting, closing, or shutting down the bus.
        #[from]
        LifecycleError,
    ),
    /// Provider selection or creation failed.
    #[error(transparent)]
    Provider(
        /// Failure resolving or constructing the selected provider.
        #[from]
        ProviderError,
    ),
}
