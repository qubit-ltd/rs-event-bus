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
use crate::error::ReceiveError;
use crate::error::SettlementError;
use crate::error::SubscribeError;

/// A typed error from any event bus operation.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EventBusError {
    /// Invalid caller configuration.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// Required provider capability is missing.
    #[error(transparent)]
    Capability(#[from] CapabilityError),
    /// A payload codec failed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// Publishing failed.
    #[error(transparent)]
    Publish(#[from] PublishError),
    /// Subscription creation failed.
    #[error(transparent)]
    Subscribe(#[from] SubscribeError),
    /// Receiving failed.
    #[error(transparent)]
    Receive(#[from] ReceiveError),
    /// Delivery processing failed.
    #[error(transparent)]
    Delivery(#[from] DeliveryError),
    /// Delivery settlement failed.
    #[error(transparent)]
    Settlement(#[from] SettlementError),
    /// A lifecycle operation failed.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// Provider selection or creation failed.
    #[error(transparent)]
    Provider(#[from] ProviderError),
}
