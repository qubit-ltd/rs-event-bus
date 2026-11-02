// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Combination context attached to conformance case results.

use crate::model::SubscriptionDurability;
use crate::spi::PayloadModes;

/// Identifies the API and capability combination exercised by a conformance
/// case.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::conformance::ConformanceCaseContext;
///
/// fn describe(context: &ConformanceCaseContext) -> (&'static str, &'static str) {
///     (context.api(), match context.payload_mode() {
///         qubit_event_bus::spi::PayloadModes::Native => "native",
///         qubit_event_bus::spi::PayloadModes::Encoded => "encoded",
///         qubit_event_bus::spi::PayloadModes::NativeAndEncoded => "both",
///         _ => "other",
///     })
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConformanceCaseContext {
    api: &'static str,
    payload_mode: PayloadModes,
    durability: SubscriptionDurability,
}

impl ConformanceCaseContext {
    pub(super) fn new(api: &'static str, payload_mode: PayloadModes, durability: SubscriptionDurability) -> Self {
        Self {
            api,
            payload_mode,
            durability,
        }
    }

    /// Returns `"sync"` or `"async"` for the SPI runner.
    #[must_use]
    pub fn api(&self) -> &'static str {
        self.api
    }

    /// Returns the single payload representation used by this probe.
    pub fn payload_mode(&self) -> PayloadModes {
        self.payload_mode
    }

    /// Returns the subscription durability used by this probe.
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }
}
