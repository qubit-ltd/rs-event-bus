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
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OrderingKey(Box<str>);

impl OrderingKey {
    /// Creates a nonblank ordering key, or returns `None` for blank values,
    /// surrounding whitespace, or control characters.
    pub fn new(value: &str) -> Option<Self> {
        is_nonblank_without_controls(value).then(|| Self(value.into()))
    }

    /// Returns the key string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
