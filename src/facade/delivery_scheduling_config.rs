// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated facade delivery scheduling limits.

use std::num::NonZeroUsize;

use crate::error::ConfigurationError;

/// Independent positive limits on execution, ownership and subscriptions.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::facade::DeliverySchedulingConfig;
///
/// let config = DeliverySchedulingConfig::default();
/// assert_eq!(config.max_running_handlers().get(), 4);
/// assert!(config.max_owned_per_subscription() <= config.max_owned_deliveries());
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliverySchedulingConfig {
    /// Maximum simultaneous handler executions.
    max_running_handlers: NonZeroUsize,
    /// Maximum deliveries owned across receive, queue, handler and settlement
    /// phases.
    max_owned_deliveries: NonZeroUsize,
    /// Maximum deliveries owned by one subscription.
    max_owned_per_subscription: NonZeroUsize,
    /// Maximum registered receiver owners or asynchronous sessions.
    max_subscriptions: NonZeroUsize,
}

impl DeliverySchedulingConfig {
    /// Creates a validated delivery scheduling policy.
    ///
    /// # Parameters
    /// - `max_running_handlers`: Maximum simultaneous handler executions.
    /// - `max_owned_deliveries`: Maximum deliveries owned across receive,
    ///   queue, handler and settlement phases.
    /// - `max_owned_per_subscription`: Maximum deliveries owned by one
    ///   subscription.
    /// - `max_subscriptions`: Maximum registered receiver owners or
    ///   asynchronous sessions.
    ///
    /// # Returns
    /// The validated policy.
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` when `max_running_handlers`
    /// exceeds `max_owned_deliveries`.
    /// Returns `ConfigurationError::InvalidField` when
    /// `max_owned_per_subscription` exceeds `max_owned_deliveries`.
    pub fn new(
        max_running_handlers: NonZeroUsize,
        max_owned_deliveries: NonZeroUsize,
        max_owned_per_subscription: NonZeroUsize,
        max_subscriptions: NonZeroUsize,
    ) -> Result<Self, ConfigurationError> {
        if max_running_handlers > max_owned_deliveries {
            return Err(ConfigurationError::InvalidField {
                field: "max_running_handlers",
                message: "must not exceed max_owned_deliveries".into(),
            });
        }
        if max_owned_per_subscription > max_owned_deliveries {
            return Err(ConfigurationError::InvalidField {
                field: "max_owned_per_subscription",
                message: "must not exceed max_owned_deliveries".into(),
            });
        }
        Ok(Self {
            max_running_handlers,
            max_owned_deliveries,
            max_owned_per_subscription,
            max_subscriptions,
        })
    }

    /// Returns maximum simultaneous handler executions.
    ///
    /// # Returns
    /// Maximum simultaneous handler executions.
    #[must_use]
    #[inline]
    pub const fn max_running_handlers(&self) -> NonZeroUsize {
        self.max_running_handlers
    }

    /// Returns maximum deliveries owned across receive, queue, handler and
    /// settlement phases.
    ///
    /// # Returns
    /// Maximum deliveries owned across receive, queue, handler and settlement
    /// phases.
    #[must_use]
    #[inline]
    pub const fn max_owned_deliveries(&self) -> NonZeroUsize {
        self.max_owned_deliveries
    }

    /// Returns maximum deliveries owned by one subscription.
    ///
    /// # Returns
    /// Maximum deliveries owned by one subscription.
    #[must_use]
    #[inline]
    pub const fn max_owned_per_subscription(&self) -> NonZeroUsize {
        self.max_owned_per_subscription
    }

    /// Returns maximum registered receiver owners or asynchronous sessions.
    ///
    /// # Returns
    /// Maximum registered receiver owners or asynchronous sessions.
    #[must_use]
    #[inline]
    pub const fn max_subscriptions(&self) -> NonZeroUsize {
        self.max_subscriptions
    }
}

impl Default for DeliverySchedulingConfig {
    /// Returns the standard finite facade policy.
    fn default() -> Self {
        Self {
            max_running_handlers: NonZeroUsize::new(4).expect("positive default limit"),
            max_owned_deliveries: NonZeroUsize::new(256).expect("positive default limit"),
            max_owned_per_subscription: NonZeroUsize::new(32).expect("positive default limit"),
            max_subscriptions: NonZeroUsize::new(256).expect("positive default limit"),
        }
    }
}
