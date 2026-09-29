// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated MIME content type.

use std::borrow::Cow;

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
/// assert_eq!(content_type, ContentType::APPLICATION_JSON);
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ContentType(
    /// The validated MIME type, borrowed when static and owned otherwise.
    Cow<'static, str>,
);

impl ContentType {
    /// Creates a MIME content type from a static string without allocating.
    ///
    /// The value must contain exactly one slash, with no whitespace or
    /// parameters. Each component may contain ASCII letters, digits, `-`, `_`,
    /// `+`, or `.`.
    ///
    /// # Parameters
    /// - `value`: the static MIME type string to validate and borrow.
    ///
    /// # Returns
    /// A content type borrowing `value` without allocating.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, if either component
    /// is empty or contains a character outside the accepted ASCII token set.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::model::ContentType;
    /// const INVALID_CONTENT_TYPE: ContentType = ContentType::new_static("text/");
    /// ```
    #[must_use]
    pub const fn new_static(value: &'static str) -> Self {
        assert!(is_valid_content_type(value), "invalid content type");
        Self(Cow::Borrowed(value))
    }

    /// The `text/plain` content type.
    pub const TEXT_PLAIN: Self = Self::new_static("text/plain");

    /// The `text/html` content type.
    pub const TEXT_HTML: Self = Self::new_static("text/html");

    /// The `text/csv` content type.
    pub const TEXT_CSV: Self = Self::new_static("text/csv");

    /// The `text/xml` content type.
    pub const TEXT_XML: Self = Self::new_static("text/xml");

    /// The `application/json` content type.
    pub const APPLICATION_JSON: Self = Self::new_static("application/json");

    /// The `application/xml` content type.
    pub const APPLICATION_XML: Self = Self::new_static("application/xml");

    /// The `application/octet-stream` content type.
    pub const APPLICATION_OCTET_STREAM: Self = Self::new_static("application/octet-stream");

    /// The `application/cbor` content type.
    pub const APPLICATION_CBOR: Self = Self::new_static("application/cbor");

    /// The `application/protobuf` content type.
    pub const APPLICATION_PROTOBUF: Self = Self::new_static("application/protobuf");

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
        if !is_valid_content_type(value) {
            return Err(ConfigurationError::InvalidField {
                field: "content_type",
                message: "expected a MIME type".into(),
            });
        }
        Ok(Self(Cow::Owned(value.into())))
    }

    /// Returns the validated MIME type string.
    ///
    /// # Returns
    /// The content type borrowed for the lifetime of this value.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

/// Checks that `value` is a MIME type with exactly one slash.
///
/// # Parameters
/// - `value`: candidate media type without parameters.
///
/// # Returns
/// `true` for two nonempty accepted ASCII tokens separated by one slash.
#[must_use = "Use the returned query result."]
const fn is_valid_content_type(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'/' {
            let (kind, rest) = value.split_at(index);
            let subtype = rest.split_at(1).1;
            return valid_mime_token(kind) && valid_mime_token(subtype);
        }
        index += 1;
    }
    false
}

/// Checks the ASCII token syntax accepted for each MIME type component.
///
/// # Parameters
/// - `value`: candidate type or subtype component.
///
/// # Returns
/// `true` when nonempty and composed only of accepted ASCII token bytes.
const fn valid_mime_token(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return false;
    }
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'+' | b'.')) {
            return false;
        }
        index += 1;
    }
    true
}
