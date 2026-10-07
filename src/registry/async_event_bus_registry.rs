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
#[cfg(feature = "discovery")]
use qubit_spi::error::ProviderInventoryBuildError;
use qubit_spi::error::ProviderResolutionError;
use qubit_spi::error::RegistryMutationError;

use super::AsyncEventBusProvider;
use super::EventBusConfig;
use super::EventBusProviderError;
use super::EventBusSpec;
use super::async_event_bus_provider_adapter::AsyncEventBusProviderAdapter;
use crate::error::ProviderError;
use crate::facade::AsyncEventBus;
use crate::local::AsyncLocalEventBusProvider;
use crate::model::ProviderId as FacadeProviderId;
use crate::spi::AsyncEventBusSpi;

/// Mutable catalog of asynchronous event-bus providers.
///
/// Registry lookup and provider metadata operations are synchronous; only
/// creation of the provider SPI and facade is asynchronous.
///
/// # Examples
///
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::AsyncEventBusRegistry;
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let registry = AsyncEventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default()).await?;
/// bus.shutdown(ShutdownMode::Immediate).await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncEventBusRegistry {
    /// Typed asynchronous provider catalog.
    providers: AsyncProviderRegistry<EventBusSpec>,
}

impl AsyncEventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    ///
    /// # Returns
    /// An empty mutable registry.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self {
            providers: AsyncProviderRegistry::default(),
        }
    }

    /// Creates a registry with the built-in asynchronous local provider.
    ///
    /// # Returns
    /// A registry containing the asynchronous local provider.
    ///
    /// # Errors
    /// Returns a registry mutation error if the provider cannot be registered.
    pub fn with_local() -> Result<Self, RegistryMutationError> {
        let registry = Self::new();
        registry.register(AsyncLocalEventBusProvider)?;
        Ok(registry)
    }

    /// Builds a registry from linked asynchronous provider submissions.
    ///
    /// Each submitted provider uses the same adapter as
    /// [`Self::register_shared`]. Provider SPIs are created later by
    /// [`Self::create`].
    ///
    /// # Returns
    /// A registry populated from linked asynchronous provider submissions.
    ///
    /// # Errors
    /// Returns an error containing the submission source when a provider
    /// cannot be registered, including duplicate selectors.
    ///
    /// # Panics
    ///
    /// Propagates panics from submitted provider factories or descriptor
    /// callbacks.
    #[cfg(feature = "discovery")]
    pub fn discover() -> Result<Self, ProviderInventoryBuildError> {
        let providers = super::async_provider_inventory::build_registry_with(|provider| {
            Arc::new(AsyncEventBusProviderAdapter::new(provider))
        })?;
        Ok(Self { providers })
    }

    /// Registers an owned asynchronous provider.
    ///
    /// # Type Parameters
    /// - `P`: asynchronous provider factory implementing the typed definition.
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
        P: AsyncProviderDefinition<EventBusSpec>,
    {
        let provider: Arc<AsyncEventBusProvider> = Arc::new(provider);
        self.register_shared(provider)
    }

    /// Registers a shared asynchronous provider.
    ///
    /// # Parameters
    /// - `provider`: shared asynchronous factory retained by the registry.
    ///
    /// # Returns
    /// `Ok(())` when registration succeeds.
    ///
    /// # Errors
    /// Returns a registry mutation error if registration is sealed or the
    /// provider selector conflicts with an existing entry.
    pub fn register_shared(&self, provider: Arc<AsyncEventBusProvider>) -> Result<(), RegistryMutationError> {
        let adapter: Arc<dyn AsyncProviderDefinition<EventBusSpec>> =
            Arc::new(AsyncEventBusProviderAdapter::new(provider));
        self.providers.register_shared(adapter)
    }

    /// Returns provider descriptors in registration order.
    ///
    /// # Returns
    /// A snapshot of registered descriptors.
    #[must_use]
    #[inline]
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
    #[inline]
    pub fn provider_ids(&self) -> Vec<ProviderId> {
        self.providers.provider_ids()
    }

    /// Returns the default selection used when configuration has no override.
    ///
    /// # Returns
    /// The current registry selection policy.
    #[must_use]
    #[inline]
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
    /// A future resolving to a facade backed by the selected provider.
    ///
    /// # Errors
    /// Returns a resolution or provider creation error when no candidate can
    /// create an SPI satisfying the configuration.
    pub async fn create(&self, config: &EventBusConfig) -> Result<AsyncEventBus, ProviderError> {
        if let Some(selection) = config.selection() {
            return self.create_selected(selection, config).await;
        }
        let resolver = self.providers.resolve().map_err(provider_resolution_error)?;
        let spi = resolver
            .create_configured(config)
            .await
            .map_err(provider_creation_error)?;
        facade(spi, config)
    }

    /// Asynchronously creates a facade using an explicit provider selection.
    ///
    /// # Parameters
    /// - `selection`: provider selector to resolve.
    /// - `config`: capability requirements and facade settings.
    ///
    /// # Returns
    /// A future resolving to a facade backed by the selected provider.
    ///
    /// # Errors
    /// Returns a resolution or provider creation error when the selection
    /// cannot create a compatible SPI.
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
        facade(spi, config)
    }
}

impl Default for AsyncEventBusRegistry {
    /// Creates an empty registry with automatic provider selection.
    fn default() -> Self {
        Self::new()
    }
}

/// Wraps a validated asynchronous provider SPI with configured facade settings.
///
/// # Parameters
/// - `spi`: provider SPI carrying a canonical provider ID.
/// - `config`: facade settings to install.
///
/// # Returns
/// A configured asynchronous event bus, or a creation error if setup fails.
///
/// # Errors
/// Returns a provider creation error if the facade configuration is invalid.
///
/// # Panics
/// Panics if a provider adapter violates its invariant and omits the canonical
/// provider ID.
fn facade(spi: Arc<dyn AsyncEventBusSpi>, config: &EventBusConfig) -> Result<AsyncEventBus, ProviderError> {
    let provider_id: FacadeProviderId = spi
        .provider_id()
        .expect("registered provider adapters attach a canonical provider ID");
    AsyncEventBus::with_config(provider_id, spi, config.facade_config().clone()).map_err(|error| {
        ProviderError::Creation {
            source: Box::new(error),
        }
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
