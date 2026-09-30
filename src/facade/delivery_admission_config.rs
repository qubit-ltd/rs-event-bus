// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared asynchronous delivery-admission limits for a facade.

use crate::error::ConfigurationError;

/// Bounds asynchronous deliveries admitted by one facade across all
/// subscriptions.
///
/// The limit applies across the facade's subscriptions and counts deliveries
/// already queued or being handled.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::DeliveryAdmissionConfig;
///
/// let config = DeliveryAdmissionConfig::new(8).unwrap();
/// assert_eq!(config.max_in_flight(), 8);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct DeliveryAdmissionConfig {
    /// Maximum number of admitted asynchronous deliveries across subscriptions.
    max_in_flight: usize,
}

impl DeliveryAdmissionConfig {
    /// Creates a positive facade-wide in-flight delivery limit.
    ///
    /// # Parameters
    /// - `max_in_flight`: maximum admitted queued or active deliveries; must be
    ///   greater than zero.
    ///
    /// # Returns
    /// A delivery-admission policy with the requested positive limit.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `max_in_flight` when
    /// the requested limit is zero.
    pub fn new(max_in_flight: usize) -> Result<Self, ConfigurationError> {
        if max_in_flight == 0 {
            return Err(ConfigurationError::InvalidField {
                field: "max_in_flight",
                message: "must be greater than zero".into(),
            });
        }
        Ok(Self { max_in_flight })
    }

    /// Returns the maximum number of admitted asynchronous deliveries.
    ///
    /// # Returns
    /// The facade-wide bound shared by all subscriptions.
    #[must_use]
    #[inline]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
}

impl Default for DeliveryAdmissionConfig {
    /// Creates the default limit of four admitted asynchronous deliveries.
    #[inline]
    fn default() -> Self {
        Self { max_in_flight: 4 }
    }
}
