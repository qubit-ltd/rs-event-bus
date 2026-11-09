// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Finite limits for portable event headers.

use std::num::NonZeroUsize;

/// Positive limits for the number and aggregate UTF-8 bytes of portable
/// headers.
///
/// Bytes count each key and value, including restored dead-letter markers. The
/// count excludes topics, identifiers, payloads, serialization overhead, and
/// allocation overhead; it is not a process memory limit.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use qubit_event_bus::{EventBusFacadeConfig, HeaderLimits};
/// let limits = HeaderLimits::new(NonZeroUsize::new(32).unwrap(), NonZeroUsize::new(4096).unwrap());
/// let config = EventBusFacadeConfig::new().with_header_limits(limits);
/// assert_eq!(config.header_limits(), limits);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct HeaderLimits {
    /// Inclusive maximum number of portable headers.
    max_count: NonZeroUsize,
    /// Inclusive maximum sum of UTF-8 key and value bytes.
    max_bytes: NonZeroUsize,
}

impl HeaderLimits {
    /// Creates positive count and aggregate UTF-8 byte limits.
    ///
    /// # Parameters
    /// - `max_count`: Maximum number of portable headers.
    /// - `max_bytes`: Maximum sum of UTF-8 key and value bytes.
    ///
    /// # Returns
    /// A value containing the supplied positive boundaries.
    #[inline]
    pub const fn new(max_count: NonZeroUsize, max_bytes: NonZeroUsize) -> Self {
        Self { max_count, max_bytes }
    }
    /// Returns the maximum number of headers, inclusive.
    ///
    /// # Returns
    /// The configured positive header count limit.
    #[inline]
    pub const fn max_count(&self) -> NonZeroUsize {
        self.max_count
    }
    /// Returns the maximum sum of key and value UTF-8 byte lengths, inclusive.
    ///
    /// # Returns
    /// The configured positive aggregate UTF-8 byte limit.
    #[inline]
    pub const fn max_bytes(&self) -> NonZeroUsize {
        self.max_bytes
    }
}

impl Default for HeaderLimits {
    /// Returns the facade defaults of 1,024 headers and 65,536 UTF-8 bytes.
    ///
    /// # Returns
    /// Finite count and aggregate-byte limits shared by publish and receive.
    fn default() -> Self {
        Self::new(NonZeroUsize::new(1_024).unwrap(), NonZeroUsize::new(65_536).unwrap())
    }
}
