// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Portable logical subscriber identifier.

use crate::error::ConfigurationError;

/// A caller-supplied logical subscriber name, independent of subscription
/// objects.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct SubscriberId(Box<str>);

impl SubscriberId {
    /// Validates a logical subscriber name and returns its owned identifier.
    ///
    /// A valid name contains 1 to 128 UTF-8 bytes, starts with an ASCII letter
    /// or digit, and then contains only ASCII letters, digits, `.`, `_`, `-`,
    /// or `:`. Invalid input returns
    /// [`ConfigurationError::InvalidSubscriberId`].
    pub fn new(value: impl AsRef<str>) -> Result<Self, ConfigurationError> {
        let value = value.as_ref();
        let valid_len = (1..=128).contains(&value.len());
        let mut chars = value.chars();
        let valid_first = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
        let valid_rest = chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':'));
        if !valid_len || !valid_first || !valid_rest {
            return Err(ConfigurationError::invalid_subscriber_id(value));
        }
        Ok(Self(value.into()))
    }

    /// Returns the original, case-sensitive subscriber name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
