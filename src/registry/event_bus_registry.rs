// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous event-bus provider catalog and facade creation.

use std::sync::Arc;

use qubit_spi::ProviderDefinition;
use qubit_spi::ProviderDescriptor;
use qubit_spi::ProviderId;
use qubit_spi::ProviderRegistry;
use qubit_spi::ProviderSelection;
use qubit_spi::error::ProviderCreationError;
use qubit_spi::error::ProviderResolutionError;
use qubit_spi::error::RegistryMutationError;

use super::EventBusConfig;
use super::EventBusProvider;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::event_bus_provider_adapter::EventBusProviderAdapter;
use crate::error::ProviderError;
use crate::facade::EventBus;
use crate::local::LocalEventBusProvider;
use crate::model::ProviderId as FacadeProviderId;

/// Mutable catalog of synchronous event-bus providers.
///
/// The registry delegates candidate ordering and creation fallback to
/// `qubit-spi`. It never holds catalog locks while providers create SPIs.
///
/// # Examples
///
/// ```rust
/// use qubit_event_bus::EventBusRegistry;
///
/// let registry = EventBusRegistry::new();
/// assert!(registry.provider_ids().is_empty());
/// ```
pub struct EventBusRegistry {
    /// Typed synchronous provider catalog.
    providers: ProviderRegistry<EventBusSpec>,
}

impl EventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: ProviderRegistry::default(),
        }
    }

    /// Creates a registry preloaded with the built-in local provider.
    ///
    /// # Errors
    /// Returns an error if the local provider cannot be registered.
    pub fn with_local() -> Result<Self, RegistryMutationError> {
        let registry = Self::new();
        registry.register(LocalEventBusProvider)?;
        Ok(registry)
    }

    /// Registers an owned synchronous provider.
    pub fn register<P>(&self, provider: P) -> Result<(), RegistryMutationError>
    where
        P: ProviderDefinition<EventBusSpec>,
    {
        let provider: Arc<EventBusProvider> = Arc::new(provider);
        self.register_shared(provider)
    }

    /// Registers a shared synchronous provider.
    pub fn register_shared(&self, provider: Arc<EventBusProvider>) -> Result<(), RegistryMutationError> {
        let adapter: Arc<dyn ProviderDefinition<EventBusSpec>> = Arc::new(EventBusProviderAdapter::new(provider));
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
    pub fn create(&self, config: &EventBusConfig) -> Result<EventBus, ProviderError> {
        if let Some(selection) = config.selection() {
            return self.create_selected(selection, config);
        }
        let resolver = self.providers.resolve().map_err(provider_resolution_error)?;
        let spi = resolver.create_configured(config).map_err(provider_creation_error)?;
        Ok(facade(spi, config))
    }

    /// Creates a facade using a validated explicit provider selection.
    pub fn create_selected(
        &self,
        selection: &ProviderSelection,
        config: &EventBusConfig,
    ) -> Result<EventBus, ProviderError> {
        let resolver = self
            .providers
            .resolve_selected(selection)
            .map_err(provider_resolution_error)?;
        let spi = resolver.create_configured(config).map_err(provider_creation_error)?;
        Ok(facade(spi, config))
    }
}

impl Default for EventBusRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn facade(spi: Arc<dyn crate::spi::EventBusSpi>, config: &EventBusConfig) -> EventBus {
    let provider_id: FacadeProviderId = spi
        .provider_id()
        .expect("registered provider adapters attach a canonical provider ID");
    EventBus::with_config(provider_id, spi, config.facade_config().clone())
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
