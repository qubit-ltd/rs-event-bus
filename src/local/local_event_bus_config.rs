// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configuration for the process-local transport.

use std::num::NonZeroUsize;

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

/// Provider option key used to serialize the optional declared-weight budget.
const MAX_TOTAL_OUTSTANDING_WEIGHT_BYTES_OPTION: &str = "local.max_total_outstanding_weight_bytes";

/// Transport-specific configuration for the built-in in-process provider.
///
/// The default queue capacity is 1,024 per subscription; the default provider
/// limit is 65,536 accepted delivery items across one provider instance. Both
/// count queued messages and received messages that have not been settled.
/// Facade worker and handler-queue limits belong to facade configuration.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::local::LocalEventBusConfig;
///
/// let config = LocalEventBusConfig::new().queue_capacity(64);
/// assert_eq!(config.get_queue_capacity(), 64);
/// ```
#[must_use = "local transport limits must be applied to a provider"]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalEventBusConfig {
    /// Maximum queued and unsettled delivery items for each subscription.
    queue_capacity: usize,
    /// Maximum outstanding delivery items shared by all provider subscriptions.
    max_total_outstanding: usize,
    /// Optional provider-wide budget for declared native payload weight.
    max_total_outstanding_weight_bytes: Option<NonZeroUsize>,
}

impl LocalEventBusConfig {
    /// Creates local configuration with 1,024 messages per subscription and
    /// 65,536 outstanding delivery items per provider instance, without an
    /// explicit declared-weight budget.
    ///
    /// # Returns
    /// A configuration with the documented local-provider defaults.
    #[inline]
    pub const fn new() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            max_total_outstanding: DEFAULT_MAX_TOTAL_OUTSTANDING,
            max_total_outstanding_weight_bytes: None,
        }
    }

    /// Parses and validates local settings from a registry configuration.
    ///
    /// # Parameters
    /// - `config`: registry configuration containing namespaced provider
    ///   options.
    ///
    /// # Returns
    /// Validated local transport settings, using defaults for omitted options.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for unknown keys,
    /// malformed values, or zero limits.
    pub(crate) fn from_provider_options(
        config: &EventBusConfig,
    ) -> Result<Self, ConfigurationError> {
        let mut local = Self::default();
        for (key, value) in config.provider_options() {
            match key.as_str() {
                QUEUE_CAPACITY_OPTION => {
                    local.queue_capacity =
                        value
                            .parse()
                            .map_err(|_| ConfigurationError::InvalidField {
                                field: QUEUE_CAPACITY_OPTION,
                                message: "must be a positive integer".into(),
                            })?;
                }
                MAX_TOTAL_OUTSTANDING_OPTION => {
                    local.max_total_outstanding =
                        value
                            .parse()
                            .map_err(|_| ConfigurationError::InvalidField {
                                field: MAX_TOTAL_OUTSTANDING_OPTION,
                                message: "must be a positive integer".into(),
                            })?;
                }
                MAX_TOTAL_OUTSTANDING_WEIGHT_BYTES_OPTION => {
                    local.max_total_outstanding_weight_bytes = Some(value.parse().map_err(
                        |_| ConfigurationError::InvalidField {
                            field: MAX_TOTAL_OUTSTANDING_WEIGHT_BYTES_OPTION,
                            message: "must be a positive integer".into(),
                        },
                    )?);
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

    /// Returns the per-subscription pending queue limit.
    ///
    /// # Returns
    /// The configured queue capacity.
    #[must_use]
    #[inline]
    pub const fn get_queue_capacity(&self) -> usize {
        self.queue_capacity
    }

    /// Returns the maximum number of outstanding deliveries across this
    /// provider.
    ///
    /// # Returns
    /// The configured provider-wide outstanding-delivery limit.
    #[must_use]
    #[inline]
    pub const fn get_max_total_outstanding(&self) -> usize {
        self.max_total_outstanding
    }

    /// Returns the configured provider-wide declared-weight budget in bytes.
    ///
    /// # Returns
    /// `Some` for an explicit positive budget, or `None` by default.
    #[must_use]
    #[inline]
    pub const fn get_max_total_outstanding_weight_bytes(&self) -> Option<NonZeroUsize> {
        self.max_total_outstanding_weight_bytes
    }

    /// Sets the provider-wide budget for declared native payload weight.
    ///
    /// # Parameters
    /// - `capacity`: positive budget in bytes, independent of delivery counts.
    ///
    /// # Returns
    /// The configuration with the declared-weight budget set.
    #[inline]
    pub const fn max_total_outstanding_weight_bytes(mut self, capacity: NonZeroUsize) -> Self {
        self.max_total_outstanding_weight_bytes = Some(capacity);
        self
    }

    /// Returns namespaced provider options for
    /// [`EventBusConfig::with_provider_options`].
    ///
    /// # Returns
    /// A map containing both delivery limits and any explicit weight budget.
    #[must_use]
    pub fn provider_options(&self) -> ProviderOptions {
        let mut options: ProviderOptions = [
            (
                QUEUE_CAPACITY_OPTION.to_owned(),
                self.queue_capacity.to_string(),
            ),
            (
                MAX_TOTAL_OUTSTANDING_OPTION.to_owned(),
                self.max_total_outstanding.to_string(),
            ),
        ]
        .into();
        if let Some(weight) = self.max_total_outstanding_weight_bytes {
            options.insert(
                MAX_TOTAL_OUTSTANDING_WEIGHT_BYTES_OPTION.to_owned(),
                weight.to_string(),
            );
        }
        options
    }

    /// Sets the maximum number of queued or unsettled messages for each
    /// subscription.
    ///
    /// # Parameters
    /// - `capacity`: per-subscription queue and unsettled-message limit.
    ///
    /// # Returns
    /// The updated configuration. A zero value is rejected by
    /// [`Self::validate`].
    #[inline]
    pub const fn queue_capacity(mut self, capacity: usize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Sets the provider-wide limit for queued or unsettled deliveries.
    ///
    /// # Parameters
    /// - `capacity`: provider-wide outstanding-delivery limit.
    ///
    /// # Returns
    /// The updated configuration. A zero value is rejected by
    /// [`Self::validate`].
    #[inline]
    pub const fn max_total_outstanding(mut self, capacity: usize) -> Self {
        self.max_total_outstanding = capacity;
        self
    }

    /// Validates local transport configuration.
    ///
    /// # Returns
    /// `Ok(())` when both limits are positive.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] naming the limit set to
    /// zero.
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
}

impl Default for LocalEventBusConfig {
    /// Creates the default per-subscription and provider-wide queue limits.
    ///
    /// # Returns
    /// A configuration with capacities of 1,024 and 65,536 respectively.
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}
