// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider message durability capabilities.

/// Whether messages survive subscriber downtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityCapability {
    /// Messages are ephemeral.
    Ephemeral,
    /// Messages are durably retained.
    Durable,
}
