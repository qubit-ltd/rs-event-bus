// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Immutable-at-creation configuration shared with event-bus providers.

use qubit_spi::ProviderSelection;

use super::RequiredCapabilities;
use crate::facade::EventBusFacadeConfig;
use crate::model::ProviderOptions;

/// Provider selection and requirements supplied to a registry creation call.
///
/// Provider-specific options are opaque to the facade and interpreted only by
/// the selected provider. Credentials and secrets must not be stored in these
/// debuggable metadata values.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::registry::EventBusRegistry;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let registry = EventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default())?;
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Default)]
pub struct EventBusConfig {
    /// Explicit provider selection, or `None` to use the registry default.
    selection: Option<ProviderSelection>,
    /// Minimum SPI capabilities checked before returning a facade.
    required_capabilities: RequiredCapabilities,
    /// Middleware, resource, and scheduling policies installed in the facade.
    facade: EventBusFacadeConfig,
    /// Non-sensitive namespaced options interpreted by the selected provider.
    provider_options: ProviderOptions,
}

impl EventBusConfig {
    /// Returns an explicit selection, if this configuration supplies one.
    ///
    /// # Returns
    /// The configured selection, or `None` to use the registry default.
    #[must_use]
    #[inline]
    pub fn selection(&self) -> Option<&ProviderSelection> {
        self.selection.as_ref()
    }

    /// Returns the facade settings consumed when the provider SPI is wrapped.
    ///
    /// # Returns
    /// The settings copied into the created facade.
    #[must_use]
    #[inline]
    pub const fn facade_config(&self) -> &EventBusFacadeConfig {
        &self.facade
    }

    /// Returns the creation-time capability requirements.
    ///
    /// # Returns
    /// The required provider capabilities.
    #[must_use]
    #[inline]
    pub const fn required_capabilities(&self) -> RequiredCapabilities {
        self.required_capabilities
    }

    /// Returns provider-specific options without assigning them core semantics.
    ///
    /// # Returns
    /// The provider option map retained by this configuration.
    #[must_use]
    #[inline]
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }

    /// Replaces the provider selection used by registry `create`.
    ///
    /// # Parameters
    /// - `selection`: provider selection to apply when creating a facade.
    ///
    /// # Returns
    /// The updated configuration.
    #[must_use]
    #[inline]
    pub fn with_selection(mut self, selection: ProviderSelection) -> Self {
        self.selection = Some(selection);
        self
    }

    /// Replaces the settings installed in the resulting facade.
    ///
    /// # Parameters
    /// - `facade`: facade settings to install after provider creation.
    ///
    /// # Returns
    /// The updated configuration.
    #[must_use]
    #[inline]
    pub fn with_facade_config(mut self, facade: EventBusFacadeConfig) -> Self {
        self.facade = facade;
        self
    }

    /// Replaces the creation-time capability requirements.
    ///
    /// # Parameters
    /// - `required`: capabilities the selected provider must support.
    ///
    /// # Returns
    /// The updated configuration.
    #[must_use]
    #[inline]
    pub fn with_required_capabilities(mut self, required: RequiredCapabilities) -> Self {
        self.required_capabilities = required;
        self
    }

    /// Replaces opaque provider-specific options.
    ///
    /// # Parameters
    /// - `options`: values passed through to the selected provider.
    ///
    /// # Returns
    /// The updated configuration.
    #[must_use]
    #[inline]
    pub fn with_provider_options(mut self, options: ProviderOptions) -> Self {
        self.provider_options = options;
        self
    }
}
