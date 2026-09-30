// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent finite encoded payload limits for both facade directions.

use std::num::NonZeroUsize;

/// Positive encoded byte limits; native retained memory cannot be measured.
///
/// Publishing is checked after encoding and before calling the provider.
/// Receiving is checked before codec callbacks. These limits bound individual
/// encoded payloads, rather than codec allocation or total process memory.
///
/// # Examples
///
/// ```
/// use std::num::NonZeroUsize;
/// use qubit_event_bus::EventBusFacadeConfig;
/// use qubit_event_bus::PayloadLimits;
///
/// let limits = PayloadLimits::new(
///     NonZeroUsize::new(512).unwrap(),
///     NonZeroUsize::new(1_024).unwrap(),
/// );
/// let config = EventBusFacadeConfig::new().with_payload_limits(limits);
/// assert_eq!(config.payload_limits().max_receive_bytes().get(), 1_024);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub struct PayloadLimits {
    /// Maximum encoded bytes admitted before provider publication.
    max_publish_bytes: NonZeroUsize,
    /// Maximum received encoded bytes admitted before codec callbacks.
    max_receive_bytes: NonZeroUsize,
}

impl PayloadLimits {
    /// Creates independent positive limits for publishing and receiving.
    /// Exactly the limit is allowed; exceeding it rejects encoded payloads.
    ///
    /// # Parameters
    /// - `max_publish_bytes`: positive encoded byte limit for publication.
    /// - `max_receive_bytes`: positive encoded byte limit for receiving.
    ///
    /// # Returns
    /// Independent limits for the two facade boundaries.
    #[inline]
    pub const fn new(max_publish_bytes: NonZeroUsize, max_receive_bytes: NonZeroUsize) -> Self {
        Self {
            max_publish_bytes,
            max_receive_bytes,
        }
    }
    /// Returns the maximum encoded bytes admitted before provider publishing.
    ///
    /// # Returns
    /// The positive publication byte limit, inclusive of its boundary.
    #[inline]
    pub const fn max_publish_bytes(&self) -> NonZeroUsize {
        self.max_publish_bytes
    }
    /// Returns the maximum encoded bytes admitted before codec callbacks.
    ///
    /// # Returns
    /// The positive receive byte limit, inclusive of its boundary.
    #[inline]
    pub const fn max_receive_bytes(&self) -> NonZeroUsize {
        self.max_receive_bytes
    }
}

impl Default for PayloadLimits {
    /// Sets both encoded boundaries to one mebibyte.
    #[inline]
    fn default() -> Self {
        let limit = NonZeroUsize::new(1_048_576).expect("the default payload limit is positive");
        Self::new(limit, limit)
    }
}
