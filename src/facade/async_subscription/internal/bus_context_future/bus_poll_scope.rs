// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop guard for a poll-scoped bus identity.

use crate::facade::async_subscription::internal::bus_context_future::ACTIVE_BUS_POLLS;

/// Removes the poll-scoped identity when polling returns or unwinds.
pub(super) struct BusPollScope;

impl Drop for BusPollScope {
    fn drop(&mut self) {
        ACTIVE_BUS_POLLS.with(|active| {
            active.borrow_mut().pop();
        });
    }
}
