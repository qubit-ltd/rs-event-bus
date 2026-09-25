// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Subscriber delivery views and transport context.

use std::sync::Arc;

use qubit_id::Id;

use super::Acknowledgement;
use super::EventEnvelope;
use super::ProviderId;
use super::ProviderMessageMetadata;
use super::SubscriberId;

/// Transport and retry context for one subscriber attempt.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::DeliveryContext;
/// use qubit_event_bus::model::ProviderId;
/// use qubit_event_bus::model::SubscriberId;
///
/// let context = DeliveryContext::new(
///     ProviderId::new("local").unwrap(),
///     qubit_id::Id::new(1),
///     SubscriberId::new("audit").unwrap(),
/// );
/// assert_eq!(context.provider_id().as_str(), "local");
/// ```
#[derive(Clone, Debug)]
pub struct DeliveryContext {
    provider_id: ProviderId,
    subscription_id: Id,
    subscriber_id: SubscriberId,
    retry_attempt: u32,
    provider_attempt: Option<u32>,
    provider_metadata: ProviderMessageMetadata,
    can_settle: bool,
    dead_letter: bool,
}

impl DeliveryContext {
    /// Creates a context supplied by the facade after receiving an event.
    pub fn new(provider_id: ProviderId, subscription_id: Id, subscriber_id: SubscriberId) -> Self {
        Self {
            provider_id,
            subscription_id,
            subscriber_id,
            retry_attempt: 1,
            provider_attempt: None,
            provider_metadata: ProviderMessageMetadata::default(),
            can_settle: false,
            dead_letter: false,
        }
    }
    /// Sets the facade attempt number supplied by the delivery pipeline.
    pub fn with_retry_attempt(mut self, value: u32) -> Self {
        self.retry_attempt = value;
        self
    }
    /// Sets the provider attempt number, when the provider supplies one.
    pub fn with_provider_attempt(mut self, value: u32) -> Self {
        self.provider_attempt = Some(value);
        self
    }
    /// Replaces non-sensitive provider message metadata.
    pub fn with_provider_metadata(mut self, value: ProviderMessageMetadata) -> Self {
        self.provider_metadata = value;
        self
    }
    /// Records whether the provider supports settling this delivery.
    pub fn with_settlement(mut self, value: bool) -> Self {
        self.can_settle = value;
        self
    }
    /// Marks this delivery as a dead letter.
    pub fn as_dead_letter(mut self) -> Self {
        self.dead_letter = true;
        self
    }
    /// Returns the source provider ID.
    #[must_use]
    #[inline]
    pub fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }
    /// Returns the bus-local subscription object ID.
    #[must_use = "the subscription ID identifies this bus-local subscription"]
    #[inline]
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }
    /// Returns the logical subscriber ID.
    #[must_use = "the subscriber ID identifies the logical consumer"]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns the facade retry attempt, starting at one.
    #[must_use]
    #[inline]
    pub fn retry_attempt(&self) -> u32 {
        self.retry_attempt
    }
    /// Returns the provider attempt, or `None` when unavailable.
    #[must_use]
    #[inline]
    pub fn provider_attempt(&self) -> Option<u32> {
        self.provider_attempt
    }
    /// Returns provider-supplied non-sensitive metadata.
    #[must_use]
    #[inline]
    pub fn provider_metadata(&self) -> &ProviderMessageMetadata {
        &self.provider_metadata
    }
    /// Returns whether the provider can settle this delivery.
    #[must_use]
    #[inline]
    pub fn can_settle(&self) -> bool {
        self.can_settle
    }
    /// Returns whether this delivery is a dead letter.
    #[must_use]
    #[inline]
    pub fn is_dead_letter(&self) -> bool {
        self.dead_letter
    }
}

/// A subscriber-visible event with a separate acknowledgement handle.
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
///
/// let topic = Topic::<String>::new("orders.created").unwrap();
/// let event = Arc::new(EventEnvelope::new(topic, "order-1".to_owned()).unwrap());
/// let context = DeliveryContext::new(
///     ProviderId::new("local").unwrap(),
///     qubit_id::Id::new(1),
///     SubscriberId::new("audit").unwrap(),
/// );
/// let delivery = Delivery::new(event, context);
/// assert_eq!(delivery.payload(), "order-1");
/// ```
pub struct Delivery<T: 'static> {
    event: Arc<EventEnvelope<T>>,
    context: DeliveryContext,
    acknowledgement: Acknowledgement,
}

impl<T: 'static> Clone for Delivery<T> {
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
    pub fn new(event: Arc<EventEnvelope<T>>, context: DeliveryContext) -> Self {
        Self {
            event,
            context,
            acknowledgement: Acknowledgement::new(),
        }
    }
    /// Returns the payload without cloning it.
    #[must_use]
    #[inline]
    pub fn payload(&self) -> &T {
        self.event.payload()
    }
    /// Returns the received envelope.
    #[must_use]
    #[inline]
    pub fn event(&self) -> &EventEnvelope<T> {
        &self.event
    }
    /// Returns a shared owner for use by retry and dead-letter policies.
    pub(crate) fn event_arc(&self) -> Arc<EventEnvelope<T>> {
        self.event.clone()
    }
    /// Creates a fresh per-attempt acknowledgement while retaining event
    /// context.
    pub(crate) fn next_attempt(&self, retry_attempt: u32) -> Self {
        Self {
            event: self.event.clone(),
            context: self.context.clone().with_retry_attempt(retry_attempt),
            acknowledgement: Acknowledgement::new(),
        }
    }
    /// Returns provider and subscriber context.
    #[must_use]
    #[inline]
    pub fn context(&self) -> &DeliveryContext {
        &self.context
    }
    /// Returns the shared ACK/NACK handle.
    #[must_use]
    #[inline]
    pub fn acknowledgement(&self) -> &Acknowledgement {
        &self.acknowledgement
    }
}
