// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-identity proxy for a synchronous SPI output.

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
    inner: std::sync::Arc<dyn EventBusSpi>,
}

impl IdentifiedEventBusSpi {
    /// Binds one validated provider identity to its concrete SPI output.
    pub(crate) fn new(provider_id: ProviderId, inner: std::sync::Arc<dyn EventBusSpi>) -> Self {
        Self { provider_id, inner }
    }
}

impl EventBusSpi for IdentifiedEventBusSpi {
    fn provider_id(&self) -> Option<ProviderId> {
        Some(self.provider_id.clone())
    }

    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        self.inner.subscribe(request)
    }

    fn wait_for_topic_idle(&self, topic: &TopicAddress, timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        self.inner.wait_for_topic_idle(topic, timeout)
    }

    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
