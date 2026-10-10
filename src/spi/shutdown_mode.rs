// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! SPI shutdown policy.

use std::time::Duration;

/// Provider shutdown behavior requested by its owning facade.
///
/// For the synchronous [`crate::EventBus`] facade, `Graceful` bounds how long
/// the caller waits for the complete shutdown sequence. If that deadline
/// expires, the caller receives [`crate::ShutdownError::TimedOut`] while a
/// background coordinator continues cleanup and the bus rejects new operations.
/// A later shutdown call can wait again, or `Immediate` can strengthen the
/// active attempt. Rust cannot forcibly stop a blocked synchronous provider
/// call or handler. Async facade shutdown is driven by its returned future and
/// may be cancelled by dropping that future.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::registry::EventBusRegistry;
/// use qubit_event_bus::spi::ShutdownMode;
/// use qubit_event_bus::spi::ShutdownOutcome;
///
/// let registry = EventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default())?;
/// let report = bus.shutdown(ShutdownMode::Graceful {
///     timeout: Duration::from_secs(5),
/// })?;
/// assert_eq!(report.outcome, ShutdownOutcome::Complete);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum ShutdownMode {
    /// Drain provider work for at most the given time.
    Graceful {
        /// Maximum time for the complete facade shutdown, including receiver
        /// close, active-work coordination, and provider SPI shutdown.
        timeout: Duration,
    },
    /// Stop immediately without draining.
    Immediate,
}
