// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Metadata describing messages missed by a receiver.

/// Provider-reported loss or omission in a subscription's observed stream.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::DeliveryGap;
///
/// let gap = DeliveryGap::new("retention expired", Some(3));
/// assert_eq!(gap.missed, Some(3));
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct DeliveryGap {
    /// Human-readable provider-neutral explanation.
    pub reason: Box<str>,
    /// Optional count of messages known to have been missed.
    pub missed: Option<u64>,
}

impl DeliveryGap {
    /// Creates a provider-reported gap with an optional known missed-message
    /// count.
    ///
    /// # Parameters
    /// - `reason`: provider-neutral explanation of the gap.
    /// - `missed`: known number of missed messages, or `None` when unknown.
    ///
    /// # Returns
    /// A gap containing the supplied explanation and count.
    pub fn new(reason: impl Into<Box<str>>, missed: Option<u64>) -> Self {
        Self {
            reason: reason.into(),
            missed,
        }
    }
}
