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
/// # Type Parameters
/// - `T`: original event payload type, which must be `'static` for facade
///   delivery.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Error as IoError;
/// use std::sync::mpsc;
/// use std::sync::Arc;
/// use std::time::Duration;
///
/// use qubit_event_bus::local::LocalEventBusConfig;
/// use qubit_event_bus::model::DeadLetterEvent;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::DeadLetterPolicy;
/// use qubit_event_bus::model::FailureDirective;
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::SubscribeRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::spi::ShutdownMode;
/// use qubit_event_bus::DeliveryError;
/// use qubit_event_bus::EventBus;
///
/// struct NonClonePayload;
///
/// let bus = EventBus::local(LocalEventBusConfig::default())?;
/// let source_topic = Topic::<NonClonePayload>::new("orders.created")?;
/// let dead_letter_topic = Topic::<DeadLetterEvent<NonClonePayload>>::new("dead.events")?;
/// let (sender, receiver) = mpsc::channel();
/// let dead_letter_subscription = bus.subscribe(
///     SubscribeRequest::new("dead-letter-reader", dead_letter_topic)?,
///     move |delivery| {
///         let dead_letter = delivery.payload();
///         let original = dead_letter.original_event_arc();
///         sender
///             .send((
///                 dead_letter.original_event().id().as_str().to_owned(),
///                 Arc::strong_count(&original),
///                 dead_letter.subscriber_id().as_str().to_owned(),
///                 dead_letter.reason().to_owned(),
///             ))
///             .expect("dead-letter receiver remains available");
///     },
/// )?;
/// let source_subscription = bus.subscribe(
///     SubscribeRequest::new("failing-worker", source_topic.clone())?
///         .with_options(
///             qubit_event_bus::model::SubscribeOptions::builder()
///                 .error_handler(|_, _| FailureDirective::DeadLetter)
///                 .dead_letter(DeadLetterPolicy::with_topic_name("dead.events")?)
///                 .build(),
///         ),
///     |_| -> Result<(), DeliveryError> {
///         Err(DeliveryError::Handler {
///             source: Box::new(IoError::other("forced delivery failure")),
///         })
///     },
/// )?;
/// bus.publish(
///     PublishRequest::builder()
///         .topic(source_topic)
///         .payload(NonClonePayload)
///         .event_id(EventId::new("event-1")?)
///         .build()?,
/// )?;
/// let (event_id, owners, subscriber_id, reason) =
///     receiver.recv_timeout(Duration::from_secs(2))?;
/// assert_eq!(event_id, "event-1");
/// assert!(owners >= 2);
/// assert_eq!(subscriber_id, "failing-worker");
/// assert!(reason.contains("forced delivery failure"));
/// source_subscription.cancel()?;
/// dead_letter_subscription.cancel()?;
/// bus.shutdown(ShutdownMode::Immediate)?;
/// # Ok(())
/// # }
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
