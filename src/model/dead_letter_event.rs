// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Typed facade-generated dead-letter event payload.

use std::sync::Arc;

use super::EventEnvelope;
use super::SubscriberId;

/// Payload published by the facade when a subscription dead-letters an event.
///
/// The original event is shared rather than cloned, so dead-lettering does not
/// add a `Clone` requirement to the original payload type. Applications can
/// subscribe to `DeadLetterEvent<T>` on the configured dead-letter topic and
/// can register a codec for this type when using encoded transports.
pub struct DeadLetterEvent<T: 'static> {
    original_event: Arc<EventEnvelope<T>>,
    subscriber_id: SubscriberId,
    reason: Box<str>,
}

impl<T: 'static> DeadLetterEvent<T> {
    /// Creates a dead-letter view over an existing event.
    pub(crate) fn new(original_event: Arc<EventEnvelope<T>>, subscriber_id: SubscriberId, reason: Box<str>) -> Self {
        Self {
            original_event,
            subscriber_id,
            reason,
        }
    }

    /// Returns the immutable original event envelope.
    #[must_use]
    pub fn original_event(&self) -> &EventEnvelope<T> {
        &self.original_event
    }

    /// Returns another shared reference to the original event without cloning
    /// its payload.
    pub fn original_event_arc(&self) -> Arc<EventEnvelope<T>> {
        Arc::clone(&self.original_event)
    }

    /// Returns the logical subscriber that reached its terminal failure policy.
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns the display form of the terminal processing error.
    ///
    /// Treat this text as untrusted diagnostic content when rendering it in
    /// logs or other externally visible sinks.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}
