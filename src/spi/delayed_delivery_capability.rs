// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider delayed-delivery capabilities.

/// Whether the provider natively delays message visibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DelayedDeliveryCapability {
    /// No native delayed delivery.
    None,
    /// Native delayed delivery is supported.
    Native,
}
