// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configuration for the process-local transport.

use crate::error::ConfigurationError;
use crate::model::ProviderOptions;
use crate::registry::EventBusConfig;

/// Default number of outstanding messages allowed per subscription.
const DEFAULT_QUEUE_CAPACITY: usize = 1024;
/// Provider option key used to serialize the local queue limit.
const QUEUE_CAPACITY_OPTION: &str = "local.queue_capacity";

/// Transport-specific configuration for the built-in in-process provider.
///
/// Queue capacity applies independently to each subscription and counts
/// queued messages plus received messages that have not been settled. Facade
/// worker and handler-queue limits belong to facade configuration instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEventBusConfig {
    queue_capacity: usize,
}

impl LocalEventBusConfig {
    /// Creates local configuration with a bounded queue of 1024 messages per
    /// subscription.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
        }
    }

    /// Sets the maximum number of queued or unsettled messages for each
    /// subscription.
    #[must_use]
    pub const fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Returns the per-subscription pending queue limit.
    #[must_use]
    pub const fn get_queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Returns namespaced provider options for
    /// [`EventBusConfig::with_provider_options`].
    ///
    /// # Returns
    /// A provider-options map containing the local queue capacity.
    #[must_use]
    pub fn provider_options(&self) -> ProviderOptions {
        [(QUEUE_CAPACITY_OPTION.to_owned(), self.queue_capacity.to_string())].into()
    }

    /// Validates local transport configuration.
    ///
    /// # Errors
    /// Returns `ConfigurationError` when queue capacity is zero.
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.queue_capacity == 0 {
            return Err(ConfigurationError::InvalidField {
                field: QUEUE_CAPACITY_OPTION,
                message: "must be greater than zero".into(),
            });
        }
        Ok(())
    }

    /// Parses and validates local settings from a registry configuration.
    ///
    /// # Errors
    /// Returns `ConfigurationError` for unknown, malformed, or invalid local
    /// provider options.
    pub(crate) fn from_provider_options(config: &EventBusConfig) -> Result<Self, ConfigurationError> {
        let mut local = Self::default();
        for (key, value) in config.provider_options() {
            if key != QUEUE_CAPACITY_OPTION {
                return Err(ConfigurationError::InvalidField {
                    field: "provider_options",
                    message: format!("unknown local provider option `{key}`").into(),
                });
            }
            local.queue_capacity = value.parse().map_err(|_| ConfigurationError::InvalidField {
                field: QUEUE_CAPACITY_OPTION,
                message: "must be a positive integer".into(),
            })?;
        }
        local.validate()?;
        Ok(local)
    }
}

impl Default for LocalEventBusConfig {
    fn default() -> Self {
        Self::new()
    }
}
