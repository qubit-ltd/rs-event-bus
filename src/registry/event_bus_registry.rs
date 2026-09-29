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
#[cfg(feature = "discovery")]
use qubit_spi::error::ProviderInventoryBuildError;
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
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::EventBusRegistry;
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let registry = EventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default())?;
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
pub struct EventBusRegistry {
    /// Typed synchronous provider catalog.
    providers: ProviderRegistry<EventBusSpec>,
}

impl EventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    ///
    /// # Returns
    /// An empty mutable registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            providers: ProviderRegistry::default(),
        }
    }

    /// Builds a registry from linked synchronous provider submissions.
    ///
    /// Each submitted provider uses the same adapter as
    /// [`Self::register_shared`].
    ///
    /// # Returns
    /// A registry populated from linked provider submissions.
    ///
    /// # Errors
    /// Returns an error containing the submission source when a provider cannot
    /// be registered, including duplicate selectors.
    ///
    /// # Panics
    ///
    /// Propagates panics from submitted provider factories or descriptor
    /// callbacks. Event-bus SPIs are created later by [`Self::create`].
    #[cfg(feature = "discovery")]
    pub fn discover() -> Result<Self, ProviderInventoryBuildError> {
        let providers = super::sync_provider_inventory::build_registry_with(|provider| {
            Arc::new(EventBusProviderAdapter::new(provider))
        })?;
        Ok(Self { providers })
    }

    /// Creates a registry preloaded with the built-in local provider.
    ///
    /// # Returns
    /// A registry containing the local provider.
    ///
    /// # Errors
    /// Returns an error if the local provider cannot be registered.
    pub fn with_local() -> Result<Self, RegistryMutationError> {
        let registry = Self::new();
        registry.register(LocalEventBusProvider)?;
        Ok(registry)
    }

    /// Registers an owned synchronous provider.
    ///
    /// # Type Parameters
    /// - `P`: provider factory implementing the typed provider definition.
    ///
    /// # Parameters
    /// - `provider`: factory retained by the registry.
    ///
    /// # Returns
    /// `Ok(())` when registration succeeds.
    ///
    /// # Errors
    /// Returns a registry mutation error if registration is sealed or the
    /// provider selector conflicts with an existing entry.
    pub fn register<P>(&self, provider: P) -> Result<(), RegistryMutationError>
    where
        P: ProviderDefinition<EventBusSpec>,
    {
        let provider: Arc<EventBusProvider> = Arc::new(provider);
        self.register_shared(provider)
    }

    /// Registers a shared synchronous provider.
    ///
    /// # Parameters
    /// - `provider`: shared factory retained by the registry.
    ///
    /// # Returns
    /// `Ok(())` when registration succeeds.
    ///
    /// # Errors
    /// Returns a registry mutation error if registration is sealed or the
    /// provider selector conflicts with an existing entry.
    pub fn register_shared(&self, provider: Arc<EventBusProvider>) -> Result<(), RegistryMutationError> {
        let adapter: Arc<dyn ProviderDefinition<EventBusSpec>> = Arc::new(EventBusProviderAdapter::new(provider));
        self.providers.register_shared(adapter)
    }

    /// Returns provider descriptors in registration order.
    ///
    /// # Returns
    /// A snapshot of registered descriptors.
    #[must_use]
    pub fn descriptors(&self) -> Vec<ProviderDescriptor> {
        self.providers.descriptors()
    }

    /// Reports whether the registry rejects further configuration mutations.
    ///
    /// # Returns
    /// `true` if the registry has been sealed.
    #[must_use]
    #[inline]
    pub fn is_sealed(&self) -> bool {
        self.providers.is_sealed()
    }

    /// Returns registered canonical provider IDs in registration order.
    ///
    /// # Returns
    /// A snapshot of canonical provider IDs.
    #[must_use]
    pub fn provider_ids(&self) -> Vec<ProviderId> {
        self.providers.provider_ids()
    }

    /// Returns the default selection used when configuration has no override.
    ///
    /// # Returns
    /// The current registry selection policy.
    #[must_use]
    pub fn default_selection(&self) -> ProviderSelection {
        self.providers.default_selection()
    }

    /// Replaces the registry's default provider selection.
    ///
    /// # Parameters
    /// - `selection`: new default provider selection.
    ///
    /// # Returns
    /// `Ok(())` when the selection is updated.
    ///
    /// # Errors
    /// Returns a registry mutation error when the registry has been sealed.
    pub fn set_default_selection(&self, selection: ProviderSelection) -> Result<(), RegistryMutationError> {
        self.providers.set_default_selection(selection)
    }

    /// Prevents further provider registration and default-selection changes.
    ///
    /// # Side Effects
    /// Mutates the registry so later configuration mutations are rejected.
    pub fn seal(&self) {
        self.providers.seal();
    }

    /// Resolves the configuration's selection or current default selection.
    ///
    /// # Parameters
    /// - `config`: provider selection, capability requirements, and facade
    ///   settings for this instance.
    ///
    /// # Returns
    /// A facade backed by the selected provider.
    ///
    /// # Errors
    /// Returns a resolution or provider creation error when no candidate can
    /// create an SPI satisfying the configuration.
    pub fn create(&self, config: &EventBusConfig) -> Result<EventBus, ProviderError> {
        if let Some(selection) = config.selection() {
            return self.create_selected(selection, config);
        }
        let resolver = self.providers.resolve().map_err(provider_resolution_error)?;
        let spi = resolver.create_configured(config).map_err(provider_creation_error)?;
        facade(spi, config)
    }

    /// Creates a facade using a validated explicit provider selection.
    ///
    /// # Parameters
    /// - `selection`: provider selector to resolve.
    /// - `config`: capability requirements and facade settings.
    ///
    /// # Returns
    /// A facade backed by the selected provider.
    ///
    /// # Errors
    /// Returns a resolution or provider creation error when the selection
    /// cannot create a compatible SPI.
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
        facade(spi, config)
    }
}

impl Default for EventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps a validated provider SPI with the configured facade settings.
///
/// # Parameters
/// - `spi`: provider SPI carrying a canonical provider ID.
/// - `config`: facade settings to install.
///
/// # Returns
/// A configured event bus, or a creation error if facade setup fails.
///
/// # Errors
/// Returns a provider creation error if the facade configuration is invalid.
///
/// # Panics
/// Panics if a provider adapter violates its invariant and omits the canonical
/// provider ID.
fn facade(spi: Arc<dyn crate::spi::EventBusSpi>, config: &EventBusConfig) -> Result<EventBus, ProviderError> {
    let provider_id: FacadeProviderId = spi
        .provider_id()
        .expect("registered provider adapters attach a canonical provider ID");
    EventBus::with_config(provider_id, spi, config.facade_config().clone()).map_err(|error| ProviderError::Creation {
        source: Box::new(error),
    })
}

/// Converts a provider catalog resolution failure to the facade error type.
///
/// # Parameters
/// - `error`: provider resolution failure to retain as the source.
///
/// # Returns
/// A provider resolution error with the original source attached.
fn provider_resolution_error(error: ProviderResolutionError) -> ProviderError {
    ProviderError::Resolution {
        source: Box::new(error),
    }
}

/// Converts a provider creation failure to the facade error type.
///
/// # Parameters
/// - `error`: provider creation failure to retain as the source.
///
/// # Returns
/// A provider creation error with the original source attached.
fn provider_creation_error(error: ProviderCreationError<EventBusProviderError>) -> ProviderError {
    ProviderError::Creation {
        source: Box::new(error),
    }
}
