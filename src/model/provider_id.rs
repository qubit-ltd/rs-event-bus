// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated identity of an event-bus provider.

use crate::error::ConfigurationError;

/// A validated provider identifier.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::ProviderId;
///
/// let provider = ProviderId::new("local").unwrap();
/// assert_eq!(provider.as_str(), "local");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProviderId(Box<str>);

impl ProviderId {
    /// Validates a nonblank provider identifier.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if value.is_empty() || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::InvalidField {
                field: "provider_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the provider identifier.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
