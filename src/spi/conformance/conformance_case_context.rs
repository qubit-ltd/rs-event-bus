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
    /// SPI API exercised by the runner: `"sync"` or `"async"`.
    api: &'static str,
    /// The single payload representation exercised by this probe; this does
    /// not describe the provider's full set of supported representations.
    payload_mode: PayloadModes,
    /// Subscription durability used by this probe.
    durability: SubscriptionDurability,
}

impl ConformanceCaseContext {
    /// Creates context identifying one API and capability combination.
    ///
    /// # Parameters
    /// - `api`: SPI runner API, either `"sync"` or `"async"`.
    /// - `payload_mode`: the single payload representation exercised by the
    ///   probe.
    /// - `durability`: subscription durability exercised by the probe.
    ///
    /// # Returns
    /// Context describing the probe's API and subscription configuration.
    pub(super) fn new(api: &'static str, payload_mode: PayloadModes, durability: SubscriptionDurability) -> Self {
        Self {
            api,
            payload_mode,
            durability,
        }
    }

    /// Returns `"sync"` or `"async"` for the SPI runner.
    ///
    /// # Returns
    /// The API name associated with this probe.
    #[must_use]
    pub fn api(&self) -> &'static str {
        self.api
    }

    /// Returns the single payload representation used by this probe.
    ///
    /// This value describes the probe's representation, not the provider's
    /// complete payload capability.
    ///
    /// # Returns
    /// The payload mode exercised by this probe.
    pub fn payload_mode(&self) -> PayloadModes {
        self.payload_mode
    }

    /// Returns the subscription durability used by this probe.
    ///
    /// # Returns
    /// The durability mode exercised by this probe.
    pub fn durability(&self) -> SubscriptionDurability {
        self.durability
    }
}
