// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Received events retained until their delivery tokens are settled.

use super::local_event::LocalEvent;
use super::local_settlement_state::LocalSettlementHandle;

/// One received event retained until its delivery token is settled.
///
/// # Type Parameters
/// - `D`: deadline representation used by the event's provider queue.
pub(in crate::local) struct LocalInFlight<D = std::time::Instant> {
    /// Event data needed to requeue a retry.
    pub(in crate::local) event: LocalEvent<D>,
    /// Identity shared with the opaque token to prevent token substitution.
    pub(in crate::local) settlement: LocalSettlementHandle,
}
