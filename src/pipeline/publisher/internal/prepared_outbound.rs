// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Fully validated outbound data retained across provider retry attempts.

use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::SystemTime;

use crate::model::EventId;
use crate::model::Headers;
use crate::model::PublishFailureContext;
use crate::spi::OrderingKey;
use crate::spi::OutboundMessage;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Validated publication metadata and payload prepared once for provider
/// retries.
///
/// # Type Parameters
/// - `T`: the original typed event payload retained for publish failure
///   handling. It must be `'static` because the provider retry lifecycle may
///   outlive the caller's stack frame.
#[must_use]
pub(in crate::pipeline::publisher) struct PreparedOutbound<T: 'static> {
    /// Provider address validated before retry begins.
    pub(in crate::pipeline::publisher) topic: TopicAddress,
    /// Event identity retained consistently across retry attempts.
    pub(in crate::pipeline::publisher) event_id: EventId,
    /// Original event creation timestamp.
    pub(in crate::pipeline::publisher) timestamp: SystemTime,
    /// Portable headers copied into each provider attempt.
    pub(in crate::pipeline::publisher) headers: Headers,
    /// Optional per-key ordering metadata.
    pub(in crate::pipeline::publisher) ordering_key: Option<OrderingKey>,
    /// Optional provider delivery delay.
    pub(in crate::pipeline::publisher) delay: Option<Duration>,
    /// Declared native weight copied unchanged into every retry attempt.
    pub(in crate::pipeline::publisher) native_payload_weight_bytes: Option<NonZeroUsize>,
    /// Native or encoded payload shared across attempts.
    pub(in crate::pipeline::publisher) payload: TransportPayload,
    /// Original typed event context used by error handlers.
    pub(in crate::pipeline::publisher) failure_context: PublishFailureContext<T>,
}

impl<T: 'static> PreparedOutbound<T> {
    /// Creates a fresh provider message while sharing the retained payload.
    ///
    /// # Returns
    /// A provider message whose immutable data matches every retry attempt.
    pub(in crate::pipeline::publisher) fn build(&self) -> OutboundMessage {
        let payload = match &self.payload {
            TransportPayload::Native(value) => TransportPayload::Native(value.clone()),
            TransportPayload::Encoded(value) => TransportPayload::Encoded(value.clone()),
        };
        let message = OutboundMessage::new(
            self.topic.clone(),
            self.event_id.clone(),
            self.timestamp,
            self.headers.clone(),
            self.ordering_key.clone(),
            self.delay,
            payload,
        );
        match self.native_payload_weight_bytes {
            Some(weight) => message.with_native_payload_weight_bytes(weight),
            None => message,
        }
    }
}
