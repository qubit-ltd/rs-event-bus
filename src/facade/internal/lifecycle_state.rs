// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private synchronous facade lifecycle state.

/// Internal state of one facade instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::facade) enum LifecycleState {
    /// The facade accepts new publish and subscribe calls.
    Running,
    /// New work is rejected while subscriptions and provider are closing.
    Closing,
    /// Subscription workers and provider have completed shutdown.
    Closed,
}
