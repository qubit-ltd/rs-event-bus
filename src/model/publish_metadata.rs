// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Portable metadata exposed to facade-wide publisher interceptors.

use std::collections::BTreeMap;

use crate::error::ConfigurationError;

/// Portable event headers that a facade-wide publisher interceptor may edit.
///
/// Payload, event identity, topic, timestamp, ordering key, and delay are
/// intentionally unavailable so a global interceptor cannot replace or
/// retarget a typed event. Header keys use ASCII letters, digits, `-`, `_`,
/// and `.`, and the reserved dead-letter marker cannot be changed.
#[derive(Clone, Debug, Default)]
pub struct PublishMetadata {
    headers: BTreeMap<String, String>,
}

impl PublishMetadata {
    pub(crate) fn from_headers(headers: BTreeMap<String, String>) -> Self {
        Self { headers }
    }

    /// Returns a header value, if present.
    pub fn header(&self, key: &str) -> Option<&str> {
        self.headers.get(key).map(String::as_str)
    }

    /// Returns all headers in deterministic key order.
    pub fn headers(&self) -> &BTreeMap<String, String> {
        &self.headers
    }

    /// Inserts or replaces a validated portable header.
    pub fn set_header(&mut self, key: impl Into<String>, value: impl Into<String>) -> Result<(), ConfigurationError> {
        let key = key.into();
        let value = value.into();
        if key.eq_ignore_ascii_case(super::DEAD_LETTER_HEADER)
            || key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            || value.chars().any(char::is_control)
        {
            return Err(ConfigurationError::InvalidField {
                field: "header",
                message: "header key or value is invalid or reserved".into(),
            });
        }
        self.headers.insert(key, value);
        Ok(())
    }

    /// Removes a header and returns its previous value.
    pub fn remove_header(&mut self, key: &str) -> Option<String> {
        if key.eq_ignore_ascii_case(super::DEAD_LETTER_HEADER) {
            return None;
        }
        self.headers.remove(key)
    }

    pub(crate) fn into_headers(self) -> BTreeMap<String, String> {
        self.headers
    }
}
