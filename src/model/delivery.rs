// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscriber delivery views and transport context.

use std::sync::Arc;

use super::Acknowledgement;
use super::DeliveryContext;
use super::EventEnvelope;

/// A subscriber-visible event with a separate acknowledgement handle.
///
/// # Type Parameters
/// - `T`: payload type carried by the received event.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_event_bus::model::Delivery;
/// use qubit_event_bus::model::DeliveryContext;
/// use qubit_event_bus::model::EventEnvelope;
/// use qubit_event_bus::model::ProviderId;
/// use qubit_event_bus::model::SubscriberId;
/// use qubit_event_bus::model::Topic;
/// use qubit_id::Id;
///
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// let event = Arc::new(EventEnvelope::new(topic, "order-1".to_owned()).unwrap());
/// let context = DeliveryContext::new(
///     ProviderId::new("local").unwrap(),
///     Id::new(1),
///     SubscriberId::new("audit").unwrap(),
/// );
/// let delivery = Delivery::new(event, context);
/// assert_eq!(delivery.payload(), "order-1");
/// ```
pub struct Delivery<T: 'static> {
    /// Shared event envelope retained across attempts.
    event: Arc<EventEnvelope<T>>,
    /// Provider and subscriber metadata for this attempt.
    context: DeliveryContext,
    /// Shared acknowledgement state for cloned delivery views.
    acknowledgement: Acknowledgement,
}

impl<T: 'static> Clone for Delivery<T> {
    /// Clones the event owner, context, and shared acknowledgement handle.
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            context: self.context.clone(),
            acknowledgement: self.acknowledgement.clone(),
        }
    }
}

impl<T: 'static> Delivery<T> {
    /// Creates a delivery view from an already received event and context.
    ///
    /// # Parameters
    /// - `event`: received event retained by shared ownership.
    /// - `context`: provider and subscriber metadata for this attempt.
    ///
    /// # Returns
    /// A delivery with a fresh acknowledgement handle.
    #[must_use = "Use the created delivery to process the received event."]
    pub fn new(event: Arc<EventEnvelope<T>>, context: DeliveryContext) -> Self {
        Self {
            event,
            context,
            acknowledgement: Acknowledgement::new(),
        }
    }

    /// Creates a fresh per-attempt acknowledgement while retaining event
    /// context.
    ///
    /// # Parameters
    /// - `retry_attempt`: one-based facade attempt number for the new delivery.
    ///
    /// # Returns
    /// A delivery over the same event with fresh attempt state.
    #[must_use = "Use the new delivery for the retry attempt."]
    pub(crate) fn next_attempt(&self, retry_attempt: u32) -> Self {
        Self {
            event: self.event.clone(),
            context: self.context.clone().with_retry_attempt(retry_attempt),
            acknowledgement: Acknowledgement::new(),
        }
    }

    /// Returns the payload without cloning it.
    ///
    /// # Returns
    /// The event payload borrowed from the retained envelope.
    #[must_use = "Use the returned payload."]
    #[inline]
    pub fn payload(&self) -> &T {
        self.event.payload()
    }

    /// Returns the received envelope.
    ///
    /// # Returns
    /// The envelope containing the event metadata and payload.
    #[must_use = "Use the returned event."]
    #[inline]
    pub fn event(&self) -> &EventEnvelope<T> {
        &self.event
    }

    /// Returns provider and subscriber context.
    ///
    /// # Returns
    /// The metadata associated with this delivery attempt.
    #[must_use]
    #[inline]
    pub fn context(&self) -> &DeliveryContext {
        &self.context
    }

    /// Returns the shared ACK/NACK handle.
    ///
    /// # Returns
    /// The acknowledgement handle shared by clones of this delivery.
    #[must_use]
    #[inline]
    pub fn acknowledgement(&self) -> &Acknowledgement {
        &self.acknowledgement
    }

    /// Returns a shared owner for use by retry and dead-letter policies.
    ///
    /// # Returns
    /// Another shared pointer to the retained event envelope.
    #[must_use]
    #[inline]
    pub(crate) fn event_arc(&self) -> Arc<EventEnvelope<T>> {
        self.event.clone()
    }
}
