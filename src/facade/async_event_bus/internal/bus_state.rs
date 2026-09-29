// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle values used to admit or reject asynchronous facade work.

/// Lifecycle states accepted by the asynchronous facade.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::facade) enum BusState {
    /// Publishes and subscriptions may start.
    Running,
    /// Shutdown has stopped new work and is cleaning up.
    Closing,
    /// Provider shutdown has completed.
    Closed,
}
