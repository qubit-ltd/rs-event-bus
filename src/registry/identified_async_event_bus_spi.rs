// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-identity proxy for an asynchronous SPI output.

use std::sync::Arc;

use crate::error::SpiError;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::spi::AsyncEventBusSpi;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::EventBusCapabilities;
use crate::spi::OutboundMessage;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiFuture;
use crate::spi::SpiSubscriptionRequest;

/// Delegates each asynchronous operation and records the creating provider ID.
pub(crate) struct IdentifiedAsyncEventBusSpi {
    /// Canonical identity snapshotted from provider metadata at registration.
    provider_id: ProviderId,
    /// Provider-owned implementation receiving all transport operations.
    inner: Arc<dyn AsyncEventBusSpi>,
}

impl IdentifiedAsyncEventBusSpi {
    /// Binds one validated provider identity to its asynchronous SPI output.
    pub(crate) fn new(provider_id: ProviderId, inner: Arc<dyn AsyncEventBusSpi>) -> Self {
        Self { provider_id, inner }
    }
}

impl AsyncEventBusSpi for IdentifiedAsyncEventBusSpi {
    fn provider_id(&self) -> Option<ProviderId> {
        Some(self.provider_id.clone())
    }

    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}
