// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated provider consumer-group identity.

use crate::error::ConfigurationError;
use crate::util::validated_text::is_nonblank_without_controls;

/// A validated provider consumer group name.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::ConsumerGroup;
///
/// let group = ConsumerGroup::new("billing-workers").unwrap();
/// assert_eq!(group.as_str(), "billing-workers");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct ConsumerGroup(
    /// Owned group name validated to exclude blank values and controls.
    Box<str>,
);

impl ConsumerGroup {
    /// Creates a nonblank group name without surrounding whitespace.
    ///
    /// # Parameters
    /// - `value`: group name to validate and own.
    ///
    /// # Returns
    /// The group name in an immutable owned representation.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `consumer_group` when
    /// the value is blank, has surrounding Unicode whitespace, or contains
    /// control characters.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !is_nonblank_without_controls(value) {
            return Err(ConfigurationError::InvalidField {
                field: "consumer_group",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the validated group name.
    ///
    /// # Returns
    /// The group name borrowed for the lifetime of this value.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
