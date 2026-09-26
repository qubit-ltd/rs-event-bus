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
/// Default number of queued or unsettled deliveries allowed across a provider.
const DEFAULT_MAX_TOTAL_OUTSTANDING: usize = 65_536;
/// Provider option key used to serialize the local queue limit.
const QUEUE_CAPACITY_OPTION: &str = "local.queue_capacity";
/// Provider option key used to serialize the provider-wide delivery limit.
const MAX_TOTAL_OUTSTANDING_OPTION: &str = "local.max_total_outstanding";

/// Transport-specific configuration for the built-in in-process provider.
///
/// The default queue capacity is 1,024 per subscription; the default provider
/// limit is 65,536 accepted delivery items across one provider instance. Both
/// count queued messages and received messages that have not been settled.
/// Facade worker and handler-queue limits belong to facade configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEventBusConfig {
    queue_capacity: usize,
    max_total_outstanding: usize,
}

impl LocalEventBusConfig {
    /// Creates local configuration with 1,024 messages per subscription and
    /// 65,536 outstanding delivery items per provider instance.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_total_outstanding: DEFAULT_MAX_TOTAL_OUTSTANDING,
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

    /// Sets the provider-wide limit for queued or unsettled deliveries.
    #[must_use]
    pub const fn max_total_outstanding(mut self, capacity: usize) -> Self {
        self.max_total_outstanding = capacity;
        self
    }

    /// Returns the maximum number of outstanding deliveries across this
    /// provider.
    #[must_use]
    pub const fn get_max_total_outstanding(&self) -> usize {
        self.max_total_outstanding
    }

    /// Returns namespaced provider options for
    /// [`EventBusConfig::with_provider_options`].
    ///
    /// # Returns
    /// A provider-options map containing both local delivery limits.
    #[must_use]
    pub fn provider_options(&self) -> ProviderOptions {
        [
            (QUEUE_CAPACITY_OPTION.to_owned(), self.queue_capacity.to_string()),
            (
                MAX_TOTAL_OUTSTANDING_OPTION.to_owned(),
                self.max_total_outstanding.to_string(),
            ),
        ]
        .into()
    }

    /// Validates local transport configuration.
    ///
    /// # Errors
    /// Returns `ConfigurationError` when either delivery limit is zero.
    pub fn validate(&self) -> Result<(), ConfigurationError> {
        if self.queue_capacity == 0 {
            return Err(ConfigurationError::InvalidField {
                field: QUEUE_CAPACITY_OPTION,
                message: "must be greater than zero".into(),
            });
        }
        if self.max_total_outstanding == 0 {
            return Err(ConfigurationError::InvalidField {
                field: MAX_TOTAL_OUTSTANDING_OPTION,
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
            match key.as_str() {
                QUEUE_CAPACITY_OPTION => {
                    local.queue_capacity = value.parse().map_err(|_| ConfigurationError::InvalidField {
                        field: QUEUE_CAPACITY_OPTION,
                        message: "must be a positive integer".into(),
                    })?;
                }
                MAX_TOTAL_OUTSTANDING_OPTION => {
                    local.max_total_outstanding = value.parse().map_err(|_| ConfigurationError::InvalidField {
                        field: MAX_TOTAL_OUTSTANDING_OPTION,
                        message: "must be a positive integer".into(),
                    })?;
                }
                _ => {
                    return Err(ConfigurationError::InvalidField {
                        field: "provider_options",
                        message: format!("unknown local provider option `{key}`").into(),
                    });
                }
            }
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
