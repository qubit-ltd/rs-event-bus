// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop guard for a poll-scoped bus identity.

use super::ACTIVE_BUS_POLLS;

/// Removes the poll-scoped identity when polling returns or unwinds.
#[must_use = "dropping this scope ends the poll-scoped bus identity"]
pub(super) struct BusPollScope;

impl Drop for BusPollScope {
    /// Removes the current poll's facade identity from the thread-local stack.
    fn drop(&mut self) {
        ACTIVE_BUS_POLLS.with(|active| {
            let _ = active.borrow_mut().pop();
        });
    }
}
