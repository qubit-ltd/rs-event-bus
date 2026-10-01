// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Controls whether a subscription continues after the provider reports a
/// delivery gap.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GapPolicy {
    /// Stop receiving so the caller can recover or replace the subscription.
    #[default]
    Stop,
    /// Emit a diagnostic and continue receiving subsequent messages.
    Continue,
}
