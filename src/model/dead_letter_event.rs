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
/// can register a codec for this type when using encoded transports. The
/// facade creates this payload when a delivery reaches its terminal policy;
/// consumers inspect the event received from that topic.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::DeadLetterEvent;
///
/// fn inspect_dead_letter<T: 'static>(event: &DeadLetterEvent<T>) {
///     let original = event.original_event();
///     let shared_original = event.original_event_arc();
///     let subscriber = event.subscriber_id();
///     let reason = event.reason();
///
///     assert!(std::sync::Arc::strong_count(&shared_original) >= 2);
///     let _ = (original, subscriber, reason);
/// }
/// ```
pub struct DeadLetterEvent<T: 'static> {
    /// Original event retained by shared ownership without a `Clone` bound.
    original_event: Arc<EventEnvelope<T>>,
    /// Subscriber whose terminal processing policy forwarded the event.
    subscriber_id: SubscriberId,
    /// Display text for the final processing failure.
    reason: Box<str>,
}

impl<T: 'static> DeadLetterEvent<T> {
    /// Creates a dead-letter view over an existing event.
    ///
    /// # Parameters
    /// - `original_event`: source envelope retained by the dead-letter payload.
    /// - `subscriber_id`: logical subscriber that reached its terminal policy.
    /// - `reason`: display text describing the terminal failure.
    ///
    /// # Returns
    /// A dead-letter payload sharing the original event allocation.
    #[must_use]
    pub(crate) fn new(original_event: Arc<EventEnvelope<T>>, subscriber_id: SubscriberId, reason: Box<str>) -> Self {
        Self {
            original_event,
            subscriber_id,
            reason,
        }
    }

    /// Returns the immutable original event envelope.
    ///
    /// # Returns
    /// The original event borrowed from this dead-letter payload.
    #[must_use = "the original event contains the source of this dead letter"]
    #[inline]
    pub fn original_event(&self) -> &EventEnvelope<T> {
        &self.original_event
    }

    /// Returns another shared reference to the original event without cloning
    /// its payload.
    ///
    /// # Returns
    /// A cloned shared owner of the original event.
    #[must_use]
    #[inline]
    pub fn original_event_arc(&self) -> Arc<EventEnvelope<T>> {
        Arc::clone(&self.original_event)
    }

    /// Returns the logical subscriber that reached its terminal failure policy.
    ///
    /// # Returns
    /// The subscriber identifier retained in this payload.
    #[must_use = "the subscriber ID identifies the consumer that failed"]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns the display form of the terminal processing error.
    ///
    /// Treat this text as untrusted diagnostic content when rendering it in
    /// logs or other externally visible sinks.
    ///
    /// # Returns
    /// The retained failure description.
    #[must_use]
    #[inline]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}
