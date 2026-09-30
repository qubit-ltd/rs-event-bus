// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result details from facade shutdown.

use crate::spi::ShutdownOutcome;

/// Reports provider shutdown and facade-known delivery abandonment.
///
/// A successful shutdown report describes resource cleanup. It does not claim
/// that every publication reached a handler or completed its business effect.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::spi::ShutdownMode;
/// use qubit_event_bus::spi::ShutdownOutcome;
///
/// let bus = EventBus::local(LocalEventBusConfig::default())?;
/// let report = bus.shutdown(ShutdownMode::Immediate)?;
/// assert_eq!(report.outcome, ShutdownOutcome::Complete);
/// assert_eq!(report.known_abandoned_deliveries, 0);
/// # Ok(())
/// # }
/// ```
#[must_use = "shutdown reports contain provider and delivery cleanup results"]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Final provider shutdown result.
    pub outcome: ShutdownOutcome,
    /// Number of facade-owned deliveries explicitly abandoned during shutdown.
    pub known_abandoned_deliveries: u64,
    /// Whether provider-owned ephemeral work may have been abandoned without a
    /// count that the facade can verify.
    pub provider_may_have_abandoned_deliveries: bool,
}

impl ShutdownReport {
    /// Creates a report after the facade has completed provider shutdown.
    ///
    /// # Parameters
    /// - `outcome`: provider's final shutdown outcome.
    /// - `known_abandoned_deliveries`: facade-counted deliveries abandoned.
    /// - `provider_may_have_abandoned_deliveries`: whether the provider may
    ///   have abandoned work the facade cannot count.
    ///
    /// # Returns
    /// A report combining provider outcome and facade-known cleanup details.
    pub(crate) fn new(
        outcome: ShutdownOutcome,
        known_abandoned_deliveries: u64,
        provider_may_have_abandoned_deliveries: bool,
    ) -> Self {
        Self {
            outcome,
            known_abandoned_deliveries,
            provider_may_have_abandoned_deliveries,
        }
    }
}
