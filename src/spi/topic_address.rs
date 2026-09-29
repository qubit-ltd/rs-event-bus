// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated type-erased topic address.

use crate::error::ConfigurationError;
use crate::util::validated_text::is_valid_topic_name;

/// Topic identity carried across a type-erased transport boundary.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::TopicAddress;
///
/// let address = TopicAddress::new("orders.created").unwrap();
/// assert_eq!(address.as_str(), "orders.created");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct TopicAddress(
    /// Owned portable topic name validated before crossing the transport
    /// boundary.
    Box<str>,
);

impl TopicAddress {
    /// Creates a topic address after validating its portable name.
    ///
    /// Returns [`ConfigurationError::InvalidField`] for an empty or oversized
    /// name, surrounding whitespace, or control characters.
    ///
    /// # Parameters
    /// - `value`: portable topic name to validate and own.
    ///
    /// # Returns
    /// A validated topic address.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] when the name is empty,
    /// exceeds 255 bytes, has surrounding whitespace, or contains controls.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !is_valid_topic_name(value) {
            return Err(ConfigurationError::InvalidField {
                field: "topic",
                message: "must be 1..=255 bytes without surrounding whitespace or controls".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the topic name.
    ///
    /// # Returns
    /// The validated topic name.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
