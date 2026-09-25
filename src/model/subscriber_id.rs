// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Portable logical subscriber identifier.

use std::borrow::Cow;

use crate::error::ConfigurationError;

/// A caller-supplied logical subscriber name, independent of subscription
/// objects.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct SubscriberId(Cow<'static, str>);

impl SubscriberId {
    /// Creates a subscriber identifier from a static string without allocating.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, if the value does not
    /// follow the portable subscriber-name syntax.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::SubscriberId;
    /// const INVALID_SUBSCRIBER: SubscriberId = SubscriberId::new_static("_audit");
    /// ```
    pub const fn new_static(value: &'static str) -> Self {
        assert!(Self::is_valid(value), "invalid subscriber ID");
        Self(Cow::Borrowed(value))
    }

    /// Validates a logical subscriber name and returns its owned identifier.
    ///
    /// A valid name contains 1 to 128 UTF-8 bytes, starts with an ASCII letter
    /// or digit, and then contains only ASCII letters, digits, `.`, `_`, `-`,
    /// or `:`. Invalid input returns
    /// [`ConfigurationError::InvalidSubscriberId`].
    pub fn new(value: impl AsRef<str>) -> Result<Self, ConfigurationError> {
        let value = value.as_ref();
        if !Self::is_valid(value) {
            return Err(ConfigurationError::invalid_subscriber_id(value));
        }
        Ok(Self(Cow::Owned(value.into())))
    }

    /// Checks the portable subscriber-name syntax using const-compatible byte
    /// operations.
    const fn is_valid(value: &str) -> bool {
        let bytes = value.as_bytes();
        if bytes.is_empty() || bytes.len() > 128 || !bytes[0].is_ascii_alphanumeric() {
            return false;
        }
        let mut index = 1;
        while index < bytes.len() {
            let byte = bytes[index];
            if !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-' | b':') {
                return false;
            }
            index += 1;
        }
        true
    }

    /// Returns the original, case-sensitive subscriber name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}
