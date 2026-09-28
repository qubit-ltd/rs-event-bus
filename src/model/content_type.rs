// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated MIME content type.

use crate::error::ConfigurationError;

/// A validated MIME content type used by a codec.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::ContentType;
///
/// let content_type = ContentType::new("application/json").unwrap();
/// assert_eq!(content_type.as_str(), "application/json");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContentType(
    /// The validated MIME type, stored as an owned string.
    Box<str>,
);

impl ContentType {
    /// Creates a MIME content type from two nonempty ASCII token components.
    ///
    /// The value must contain exactly one slash, with no whitespace or
    /// parameters. Each component may contain ASCII letters, digits, `-`, `_`,
    /// `+`, or `.`.
    ///
    /// # Parameters
    /// - `value`: the MIME type string to validate and copy.
    ///
    /// # Returns
    /// The validated content type, stored as an owned immutable string.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] for `content_type` when
    /// either component is empty or contains a character outside the accepted
    /// ASCII token set.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        let valid = value
            .split_once('/')
            .is_some_and(|(kind, subtype)| valid_mime_token(kind) && valid_mime_token(subtype));
        if !valid {
            return Err(ConfigurationError::InvalidField {
                field: "content_type",
                message: "expected a MIME type".into(),
            });
        }
        Ok(Self(value.into()))
    }

    /// Returns the validated MIME type string.
    ///
    /// # Returns
    /// The content type borrowed for the lifetime of this value.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Checks the ASCII token syntax accepted for each MIME type component.
fn valid_mime_token(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'.'))
}
