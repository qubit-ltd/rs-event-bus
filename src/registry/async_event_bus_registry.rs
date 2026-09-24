// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event-bus provider catalog and facade creation.

use std::sync::Arc;

use qubit_spi::AsyncProviderDefinition;
use qubit_spi::AsyncProviderRegistry;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderSelection;
use qubit_spi::error::ProviderCreationError;
use qubit_spi::error::ProviderResolutionError;
use qubit_spi::error::RegistryMutationError;

use super::AsyncEventBusProvider;
use super::EventBusConfig;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::async_event_bus_provider_adapter::AsyncEventBusProviderAdapter;
use crate::error::ProviderError;
use crate::facade::AsyncEventBus;
use crate::model::ProviderId as FacadeProviderId;
use crate::spi::AsyncEventBusSpi;

/// Mutable catalog of asynchronous event-bus providers.
///
/// Registry lookup and provider metadata operations are synchronous; only
/// creation of the provider SPI and facade is asynchronous.
///
/// # Examples
///
/// ```rust
/// use qubit_event_bus::AsyncEventBusRegistry;
///
/// let registry = AsyncEventBusRegistry::new();
/// assert!(registry.provider_ids().is_empty());
/// ```
pub struct AsyncEventBusRegistry {
    /// Typed asynchronous provider catalog.
    providers: AsyncProviderRegistry<EventBusSpec>,
}

impl AsyncEventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: AsyncProviderRegistry::default(),
        }
    }

    /// Registers an owned asynchronous provider.
    pub fn register<P>(&self, provider: P) -> Result<(), RegistryMutationError>
    where
        P: AsyncProviderDefinition<EventBusSpec>,
    {
        let provider: Arc<AsyncEventBusProvider> = Arc::new(provider);
        self.register_shared(provider)
    }

    /// Registers a shared asynchronous provider.
    pub fn register_shared(&self, provider: Arc<AsyncEventBusProvider>) -> Result<(), RegistryMutationError> {
        let adapter: Arc<dyn AsyncProviderDefinition<EventBusSpec>> =
            Arc::new(AsyncEventBusProviderAdapter::new(provider));
        self.providers.register_shared(adapter)
    }

    /// Returns provider descriptors in registration order.
    #[must_use]
    pub fn descriptors(&self) -> Vec<ProviderDescriptor> {
        self.providers.descriptors()
    }

    /// Returns registered canonical provider IDs in registration order.
    #[must_use]
    pub fn provider_ids(&self) -> Vec<ProviderId> {
        self.providers.provider_ids()
    }

    /// Returns the default selection used when configuration has no override.
    #[must_use]
    pub fn default_selection(&self) -> ProviderSelection {
        self.providers.default_selection()
    }

    /// Replaces the registry's default provider selection.
    pub fn set_default_selection(&self, selection: ProviderSelection) -> Result<(), RegistryMutationError> {
        self.providers.set_default_selection(selection)
    }

    /// Prevents further provider registration and default-selection changes.
    pub fn seal(&self) {
        self.providers.seal();
    }

    /// Reports whether the registry rejects further configuration mutations.
    #[must_use]
    pub fn is_sealed(&self) -> bool {
        self.providers.is_sealed()
    }

    /// Resolves the configuration's selection or current default selection.
    pub async fn create(&self, config: &EventBusConfig) -> Result<AsyncEventBus, ProviderError> {
        if let Some(selection) = config.selection() {
            return self.create_selected(selection, config).await;
        }
        let resolver = self.providers.resolve().map_err(provider_resolution_error)?;
        let spi = resolver
            .create_configured(config)
            .await
            .map_err(provider_creation_error)?;
        Ok(facade(spi, config))
    }

    /// Asynchronously creates a facade using an explicit provider selection.
    pub async fn create_selected(
        &self,
        selection: &ProviderSelection,
        config: &EventBusConfig,
    ) -> Result<AsyncEventBus, ProviderError> {
        let resolver = self
            .providers
            .resolve_selected(selection)
            .map_err(provider_resolution_error)?;
        let spi = resolver
            .create_configured(config)
            .await
            .map_err(provider_creation_error)?;
        Ok(facade(spi, config))
    }
}

impl Default for AsyncEventBusRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn facade(spi: Arc<dyn AsyncEventBusSpi>, config: &EventBusConfig) -> AsyncEventBus {
    let provider_id: FacadeProviderId = spi
        .provider_id()
        .expect("registered provider adapters attach a canonical provider ID");
    AsyncEventBus::with_config(provider_id, spi, config.facade_config().clone())
}

fn provider_resolution_error(error: ProviderResolutionError) -> ProviderError {
    ProviderError::Resolution {
        source: Box::new(error),
    }
}

fn provider_creation_error(error: ProviderCreationError<EventBusProviderError>) -> ProviderError {
    ProviderError::Creation {
        source: Box::new(error),
    }
}
