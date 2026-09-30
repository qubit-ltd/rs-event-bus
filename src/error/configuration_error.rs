// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Invalid event bus configuration and identifiers.

use thiserror::Error;

/// A caller-provided configuration value failed validation.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::ConfigurationError;
///
/// let error = ConfigurationError::invalid_subscriber_id("_audit");
/// assert!(matches!(error, ConfigurationError::InvalidSubscriberId { .. }));
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
#[must_use]
pub enum ConfigurationError {
    /// The logical subscriber identifier violates its portable syntax.
    #[error("invalid subscriber ID: {value:?}")]
    InvalidSubscriberId {
        /// Rejected input, retained for diagnostics.
        value: Box<str>,
    },
    /// The event identifier is empty, too long, or contains invalid whitespace.
    #[error("invalid event ID: {value:?}")]
    InvalidEventId {
        /// Rejected input, retained for diagnostics.
        value: Box<str>,
    },
    /// A required configuration field was omitted.
    #[error("missing required field {field}")]
    MissingField {
        /// Name of the missing field.
        field: &'static str,
    },
    /// A configuration field contains an invalid value.
    #[error("invalid field {field}: {message}")]
    InvalidField {
        /// Name of the invalid field.
        field: &'static str,
        /// Explanation of the rejected value.
        message: Box<str>,
    },
}

impl ConfigurationError {
    /// Reports a subscriber ID that failed portable-syntax validation.
    ///
    /// # Parameters
    /// - `value`: the rejected subscriber ID to retain for diagnostics.
    ///
    /// # Returns
    /// An error containing an owned copy of the invalid value.
    pub fn invalid_subscriber_id(value: &str) -> Self {
        Self::InvalidSubscriberId { value: value.into() }
    }

    /// Reports an event ID that failed validation.
    ///
    /// # Parameters
    /// - `value`: the rejected event ID to retain for diagnostics.
    ///
    /// # Returns
    /// An error containing an owned copy of the invalid value.
    pub fn invalid_event_id(value: &str) -> Self {
        Self::InvalidEventId { value: value.into() }
    }
}
