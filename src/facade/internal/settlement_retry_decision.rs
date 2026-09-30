// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pure settlement retry outcomes consumed by facade owners.

use std::time::Duration;

use crate::model::SettlementTermination;

/// Whether a failed settlement may retry after a bounded delay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub(in crate::facade) enum SettlementRetryDecision {
    /// Retry after this delay, subject to admission at the next attempt.
    RetryAfter(Duration),
    /// Stop settlement for this terminal reason.
    Stop(SettlementTermination),
}
