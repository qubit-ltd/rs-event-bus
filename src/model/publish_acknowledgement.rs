// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider publication admission outcomes.

use std::collections::BTreeMap;

use super::DestinationAdmission;

/// Non-sensitive metadata returned by a provider after accepting a message.
pub type ProviderMessageMetadata = BTreeMap<String, String>;

/// What the publish path has accepted, without promising handler completion.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublishAcknowledgement {
    /// A broker accepted the message; consumer identities may be unknown.
    Accepted {
        /// Provider message ID, when one is available.
        provider_message_id: Option<String>,
        /// Non-sensitive provider metadata such as partition or offset.
        metadata: ProviderMessageMetadata,
    },
    /// A local provider can report admission for individual subscribers.
    DestinationAdmissions(Vec<DestinationAdmission>),
    /// A publisher interceptor intentionally stopped dispatch.
    DroppedByInterceptor,
}

impl PublishAcknowledgement {
    /// Returns whether the publication was intentionally dropped before
    /// dispatch.
    pub fn is_dropped(&self) -> bool {
        matches!(self, Self::DroppedByInterceptor)
    }
}
