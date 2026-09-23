// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated event identifier.

use crate::error::ConfigurationError;

/// A validated event identifier carried across providers.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct EventId(Box<str>);

impl EventId {
    /// Validates and owns an event identifier.
    ///
    /// Empty identifiers, leading or trailing whitespace, control characters,
    /// and identifiers longer than 128 UTF-8 bytes return
    /// [`ConfigurationError::InvalidEventId`].
    pub fn new(value: impl AsRef<str>) -> Result<Self, ConfigurationError> {
        let value = value.as_ref();
        if !(1..=128).contains(&value.len()) || value.trim() != value || value.chars().any(char::is_control) {
            return Err(ConfigurationError::invalid_event_id(value));
        }
        Ok(Self(value.into()))
    }

    /// Returns the original event identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
