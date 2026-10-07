// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Creation-time validator for asynchronous event-bus providers.

use std::sync::Arc;

use qubit_spi::AsyncServiceProvider;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderFuture;
use qubit_spi::ProviderMetadata;
use qubit_spi::error::ProviderFailure;

use super::AsyncEventBusProvider;
use super::EventBusConfig;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::identified_async_event_bus_spi::IdentifiedAsyncEventBusSpi;
use crate::model::ProviderId;
use crate::spi::AsyncEventBusSpi;
use crate::spi::panic_boundary::catch_spi_call;

/// Validates async provider capabilities and attaches snapshotted identity.
pub(crate) struct AsyncEventBusProviderAdapter {
    /// Original asynchronous provider factory.
    provider: Arc<AsyncEventBusProvider>,
    /// Immutable metadata captured before insertion in the registry.
    descriptor: ProviderDescriptor,
    /// Facade ID corresponding to the captured canonical descriptor.
    provider_id: ProviderId,
}

impl AsyncEventBusProviderAdapter {
    /// Captures the provider descriptor once and validates its facade identity.
    ///
    /// # Parameters
    /// - `provider`: asynchronous factory whose metadata and SPI are adapted.
    ///
    /// # Returns
    /// An adapter with a stable descriptor and facade provider ID.
    ///
    /// # Panics
    /// Panics if the provider descriptor callback panics or its provider ID
    /// violates the facade's validated provider-ID invariants.
    #[must_use]
    pub(crate) fn new(provider: Arc<AsyncEventBusProvider>) -> Self {
        let descriptor = provider.descriptor();
        let provider_id = ProviderId::new(descriptor.id().as_str())
            .expect("qubit-spi provider IDs satisfy the facade ID invariants");
        Self {
            provider,
            descriptor,
            provider_id,
        }
    }
}

impl ProviderMetadata for AsyncEventBusProviderAdapter {
    /// Returns the provider metadata captured when the adapter was created.
    ///
    /// # Returns
    /// A clone of the immutable descriptor snapshot used for registry identity.
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor.clone()
    }
}

impl AsyncServiceProvider<EventBusSpec> for AsyncEventBusProviderAdapter {
    /// Creates an asynchronous SPI and validates its declared capabilities.
    ///
    /// # Parameters
    /// - `config`: facade configuration and required provider capabilities.
    ///
    /// # Returns
    /// A future resolving to the identity-tagged SPI after capability checks.
    ///
    /// # Errors
    /// Returns the provider's creation failure, a capability-query failure, or
    /// an unsupported-capability failure when the SPI cannot meet `config`.
    fn create_configured<'a>(
        &'a self,
        config: &'a EventBusConfig,
    ) -> ProviderFuture<'a, Result<Arc<dyn AsyncEventBusSpi>, ProviderFailure<EventBusProviderError>>>
    {
        Box::pin(async move {
            let spi = self.provider.create_configured(config).await?;
            let capabilities =
                catch_spi_call(self.provider_id.as_str(), "capabilities", None, || {
                    spi.capabilities()
                })
                .map_err(|error| {
                    ProviderFailure::initialization_failed(EventBusProviderError::provider(error))
                })?;
            let missing = config.required_capabilities().missing_from(capabilities);
            if !missing.is_empty() {
                return Err(ProviderFailure::unsupported(
                    EventBusProviderError::UnsupportedCapabilities { missing },
                ));
            }
            let identified: Arc<dyn AsyncEventBusSpi> = Arc::new(IdentifiedAsyncEventBusSpi::new(
                self.provider_id.clone(),
                spi,
                capabilities,
            ));
            Ok(identified)
        })
    }
}
