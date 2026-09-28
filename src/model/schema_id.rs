// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated schema identifier.

use std::borrow::Cow;

use super::validated_text::is_nonblank_without_controls;
use crate::error::ConfigurationError;

/// A validated schema identifier supplied by an application codec.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SchemaId;
///
/// let schema_id = SchemaId::new("order-v1").unwrap();
/// assert_eq!(schema_id.as_str(), "order-v1");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SchemaId(
    /// The validated identifier, borrowed when static and owned otherwise.
    Cow<'static, str>,
);

impl SchemaId {
    /// Creates a schema identifier from a static string without allocating.
    ///
    /// # Panics
    /// Panics during constant evaluation, or at runtime, if the value is empty,
    /// has surrounding Unicode whitespace, or contains a control character.
    ///
    /// # Parameters
    /// - `value`: the static schema identifier to validate and borrow.
    ///
    /// # Returns
    /// A schema identifier borrowing `value` without allocating.
    ///
    /// ```compile_fail
    /// use qubit_event_bus::model::SchemaId;
    /// const INVALID_SCHEMA: SchemaId = SchemaId::new_static("schema-v1\n");
    /// ```
    pub const fn new_static(value: &'static str) -> Self {
        assert!(is_nonblank_without_controls(value), "invalid schema ID");
        Self(Cow::Borrowed(value))
    }

    /// Creates a schema identifier by validating and copying its string.
    ///
    /// # Parameters
    /// - `value`: the schema identifier to validate and own.
    ///
    /// # Returns
    /// The owned identifier when its contents are valid.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] with field `schema_id`
    /// when `value` is blank, has surrounding Unicode whitespace, or contains
    /// a control character.
    pub fn new(value: &str) -> Result<Self, ConfigurationError> {
        if !is_nonblank_without_controls(value) {
            return Err(ConfigurationError::InvalidField {
                field: "schema_id",
                message: "must be nonblank and contain no controls".into(),
            });
        }
        Ok(Self(Cow::Owned(value.into())))
    }

    /// Returns the original schema identifier.
    ///
    /// # Returns
    /// The validated identifier borrowed for the lifetime of this value.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}
