// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Portable ordering key metadata.

use crate::util::validated_text::is_nonblank_without_controls;

/// A nonblank key used to request per-key ordering.
///
/// Keys must contain no control characters or surrounding whitespace. The
/// validated text is owned by this value and remains unchanged.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::OrderingKey;
///
/// let key = OrderingKey::new("account-42").unwrap();
/// assert_eq!(key.as_str(), "account-42");
/// ```
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
#[must_use]
pub struct OrderingKey(
    /// Owned partition key validated to be nonblank and to exclude
    /// surrounding whitespace and control characters.
    Box<str>,
);

impl OrderingKey {
    /// Creates a nonblank ordering key, or returns `None` for blank values,
    /// surrounding whitespace, or control characters.
    ///
    /// # Parameters
    /// - `value`: ordering key text to validate and own.
    ///
    /// # Returns
    /// `Some` with the validated key, or `None` when invalid.
    #[must_use]
    pub fn new(value: &str) -> Option<Self> {
        is_nonblank_without_controls(value).then(|| Self(value.into()))
    }

    /// Borrows the validated key text without allocating or changing it.
    ///
    /// # Returns
    /// The exact key text supplied to [`Self::new`], borrowed for the lifetime
    /// of `self`.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
