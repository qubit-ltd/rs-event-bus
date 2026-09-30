// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider message durability capabilities.

/// Whether messages survive subscriber downtime.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::DurabilityCapability;
///
/// let retention = match DurabilityCapability::Durable {
///     DurabilityCapability::Durable => "retained across downtime",
///     DurabilityCapability::Ephemeral => "available only while online",
///     _ => "provider-defined retention",
/// };
/// assert_eq!(retention, "retained across downtime");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityCapability {
    /// Messages are not retained across subscriber downtime.
    Ephemeral,
    /// Messages remain available after subscriber downtime under the
    /// provider's durable-retention policy.
    Durable,
}
