// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Identity of one subscriber mailbox.

use qubit_id::Id;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(in crate::local) struct MailboxKey {
    /// Bus-local subscription ID used as the mailbox identity.
    pub(in crate::local) subscription_id: Id,
}
