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
    /// Capability snapshot checked before the facade was created.
    capabilities: EventBusCapabilities,
}

impl IdentifiedAsyncEventBusSpi {
    /// Binds one validated provider identity to its asynchronous SPI output.
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
        inner: Arc<dyn AsyncEventBusSpi>,
        capabilities: EventBusCapabilities,
    ) -> Self {
        Self {
            provider_id,
            inner,
            capabilities,
        }
    }
}

impl AsyncEventBusSpi for IdentifiedAsyncEventBusSpi {
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

    /// Publishes through the provider-owned asynchronous SPI.
    ///
    /// # Parameters
    /// - `message`: transport message to forward unchanged.
    ///
    /// # Returns
    /// A future resolving to the provider's publication acknowledgement.
    ///
    /// # Errors
    /// The future returns the SPI failure produced by the wrapped provider.
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.inner.publish(message)
    }

    /// Registers a provider subscription and returns its asynchronous receiver.
    ///
    /// # Parameters
    /// - `request`: provider subscription parameters to forward unchanged.
    ///
    /// # Returns
    /// A future resolving to the provider-owned subscription receiver.
    ///
    /// # Errors
    /// The future returns the SPI failure produced by the wrapped provider.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        self.inner.subscribe(request)
    }

    /// Forwards asynchronous shutdown to the provider-owned SPI.
    ///
    /// # Parameters
    /// - `mode`: graceful or immediate shutdown behavior.
    ///
    /// # Returns
    /// A future resolving to the provider's stable shutdown outcome.
    ///
    /// # Errors
    /// The future returns the SPI failure produced by the wrapped provider.
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.inner.shutdown(mode)
    }
}
