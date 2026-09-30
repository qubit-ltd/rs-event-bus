// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-identity proxy for a synchronous SPI output.

use std::sync::Arc;
use std::time::Duration;

use crate::error::SpiError;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OutboundMessage;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

/// Delegates every operation while attaching the descriptor that created the
/// SPI.
pub(crate) struct IdentifiedEventBusSpi {
    /// Canonical identity snapshotted from provider metadata at registration.
    provider_id: ProviderId,
    /// Provider-owned implementation receiving all transport operations.
    inner: Arc<dyn EventBusSpi>,
    /// Capability snapshot checked before the facade was created.
    capabilities: EventBusCapabilities,
}

impl IdentifiedEventBusSpi {
    /// Binds one validated provider identity to its concrete SPI output.
    ///
    /// # Parameters
    /// - `provider_id`: canonical identity recorded at provider registration.
    /// - `inner`: provider implementation receiving transport operations.
    /// - `capabilities`: validated capabilities reported by the provider.
    ///
    /// # Returns
    /// An identity proxy that delegates to `inner`.
    pub(crate) fn new(
        provider_id: ProviderId,
        inner: Arc<dyn EventBusSpi>,
        capabilities: EventBusCapabilities,
    ) -> Self {
        Self {
            provider_id,
            inner,
            capabilities,
        }
    }
}

impl EventBusSpi for IdentifiedEventBusSpi {
    /// Returns the canonical identity attached during provider adaptation.
    ///
    /// # Returns
    /// The stable provider ID; this adapter always has one.
    fn provider_id(&self) -> Option<ProviderId> {
        Some(self.provider_id.clone())
    }

    /// Returns the capabilities validated by the provider adapter.
    ///
    /// # Returns
    /// The immutable capability snapshot used when the facade was created.
    #[inline]
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    /// Publishes through the provider-owned SPI.
    ///
    /// # Parameters
    /// - `message`: transport message to forward unchanged.
    ///
    /// # Returns
    /// The provider's publication acknowledgement.
    ///
    /// # Errors
    /// Returns the SPI failure produced by the wrapped provider.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }

    /// Registers a provider subscription and returns its receiver.
    ///
    /// # Parameters
    /// - `request`: provider subscription parameters to forward unchanged.
    ///
    /// # Returns
    /// The provider-owned subscription receiver.
    ///
    /// # Errors
    /// Returns the SPI failure produced by the wrapped provider.
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        self.inner.subscribe(request)
    }

    /// Queries whether the wrapped provider has drained a topic.
    ///
    /// # Parameters
    /// - `topic`: topic whose outstanding work is queried.
    /// - `timeout`: maximum provider wait, or `None` for an unbounded wait.
    ///
    /// # Returns
    /// The provider's idle result, or `None` when it cannot report topic idle.
    ///
    /// # Errors
    /// Returns the SPI failure produced by the wrapped provider.
    fn wait_for_topic_idle(&self, topic: &TopicAddress, timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        self.inner.wait_for_topic_idle(topic, timeout)
    }

    /// Forwards shutdown to the provider-owned SPI.
    ///
    /// # Parameters
    /// - `mode`: graceful or immediate shutdown behavior.
    ///
    /// # Returns
    /// The provider's stable shutdown outcome.
    ///
    /// # Errors
    /// Returns the SPI failure produced by the wrapped provider.
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
