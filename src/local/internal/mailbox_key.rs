// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Identity of one subscriber mailbox.

use crate::model::SubscriberId;
use crate::spi::TopicAddress;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(in crate::local) struct MailboxKey {
    pub(in crate::local) topic: TopicAddress,
    pub(in crate::local) subscriber: SubscriberId,
}
