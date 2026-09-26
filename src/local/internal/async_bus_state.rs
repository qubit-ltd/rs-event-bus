// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registry state shared by asynchronous local bus operations.

use std::any::TypeId;
use std::collections::HashMap;

use super::MailboxKey;
use super::async_mailbox::AsyncMailbox;
use crate::spi::ShutdownOutcome;
use crate::spi::TopicAddress;

#[derive(Default)]
pub(in crate::local) struct AsyncBusState {
    pub(in crate::local) closed: bool,
    pub(in crate::local) outcome: Option<ShutdownOutcome>,
    pub(in crate::local) mailboxes: HashMap<MailboxKey, std::sync::Arc<AsyncMailbox>>,
    pub(in crate::local) payload_types: HashMap<TopicAddress, TypeId>,
}
