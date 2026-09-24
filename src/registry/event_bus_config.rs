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
#[derive(Clone, Default)]
pub struct EventBusConfig {
    selection: Option<ProviderSelection>,
    required_capabilities: RequiredCapabilities,
    facade: EventBusFacadeConfig,
    provider_options: ProviderOptions,
}

impl EventBusConfig {
    /// Replaces the provider selection used by registry `create`.
    #[must_use]
    pub fn with_selection(mut self, selection: ProviderSelection) -> Self {
        self.selection = Some(selection);
        self
    }

    /// Returns an explicit selection, if this configuration supplies one.
    #[must_use]
    pub fn selection(&self) -> Option<&ProviderSelection> {
        self.selection.as_ref()
    }

    /// Replaces the settings installed in the resulting facade.
    #[must_use]
    pub fn with_facade_config(mut self, facade: EventBusFacadeConfig) -> Self {
        self.facade = facade;
        self
    }

    /// Returns the facade settings consumed when the provider SPI is wrapped.
    #[must_use]
    pub const fn facade_config(&self) -> &EventBusFacadeConfig {
        &self.facade
    }

    /// Replaces the creation-time capability requirements.
    #[must_use]
    pub fn with_required_capabilities(mut self, required: RequiredCapabilities) -> Self {
        self.required_capabilities = required;
        self
    }

    /// Returns the creation-time capability requirements.
    #[must_use]
    pub const fn required_capabilities(&self) -> RequiredCapabilities {
        self.required_capabilities
    }

    /// Replaces opaque provider-specific options.
    #[must_use]
    pub fn with_provider_options(mut self, options: ProviderOptions) -> Self {
        self.provider_options = options;
        self
    }

    /// Returns provider-specific options without assigning them core semantics.
    #[must_use]
    pub fn provider_options(&self) -> &ProviderOptions {
        &self.provider_options
    }
}
