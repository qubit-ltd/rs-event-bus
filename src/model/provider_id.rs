// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated identity of an event-bus provider.

use std::borrow::Cow;

use crate::error::ConfigurationError;
use crate::util::validated_text::is_nonblank_without_controls;

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
pub struct ProviderId(
    /// Validated provider name, borrowed for static IDs or owned for runtime
    /// IDs.
    Cow<'static, str>,
);

impl ProviderId {
    /// Creates a provider identifier from a static string without allocating.
    ///
    /// # Parameters
    /// - `value`: static provider identifier to validate and borrow.
    ///
    /// # Returns
    /// A provider ID borrowing the validated static string.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, if the value is empty,
    /// has surrounding Unicode whitespace, or contains a control character.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::model::ProviderId;
    /// const INVALID_PROVIDER: ProviderId = ProviderId::new_static(" local");
    /// ```
    #[must_use]
    pub const fn new_static(value: &'static str) -> Self {
        assert!(is_nonblank_without_controls(value), "invalid provider ID");
        Self(Cow::Borrowed(value))
    }

    /// Validates and owns a nonblank provider identifier.
    ///
    /// # Parameters
    /// - `value`: provider identifier to validate and retain.
    ///
    /// # Returns
    /// A provider ID stored as an owned immutable string.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `provider_id` if the
    /// value is blank, has surrounding whitespace, or contains control
    /// characters.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !is_nonblank_without_controls(value) {
            return Err(ConfigurationError::InvalidField {
                field: "provider_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(Cow::Owned(value.into())))
    }

    /// Returns the validated provider identifier.
    ///
    /// # Returns
    /// The provider identifier borrowed for the lifetime of this value.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}
