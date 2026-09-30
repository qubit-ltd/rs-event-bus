// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-publication data prepared once before provider retries.

use super::PreparedOutbound;
use crate::model::EventId;
use crate::model::PublishOptions;

/// Input identity, options, and immutable outbound retained across attempts.
#[must_use]
pub(in crate::pipeline::publisher) struct PreparedPublish<T: 'static> {
    /// Identity supplied before any interceptor executes.
    pub(in crate::pipeline::publisher) input_event_id: EventId,
    /// Callbacks and retry controls attached to the original publication.
    pub(in crate::pipeline::publisher) options: PublishOptions<T>,
    /// Validated outbound and original failure context shared by retries.
    pub(in crate::pipeline::publisher) outbound: PreparedOutbound<T>,
}
