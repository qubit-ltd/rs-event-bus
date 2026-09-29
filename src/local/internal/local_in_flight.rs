// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Focused owner for local provider state.

use super::LocalEvent;
use super::LocalSettlementHandle;

/// One received event retained until its delivery token is settled.
pub(in crate::local) struct LocalInFlight {
    /// Event data needed to requeue a retry.
    pub(in crate::local) event: LocalEvent,
    /// Identity shared with the opaque token to prevent token substitution.
    pub(in crate::local) settlement: LocalSettlementHandle,
}
