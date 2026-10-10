// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider and retry metadata attached to a subscriber delivery.

use qubit_id::Id;

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
/// use qubit_id::Id;
///
/// let context = DeliveryContext::new(
///     ProviderId::new("local").unwrap(),
///     Id::new(1),
///     SubscriberId::new("audit").unwrap(),
/// );
/// assert_eq!(context.provider_id().as_str(), "local");
/// ```
#[derive(Clone, Debug)]
pub struct DeliveryContext {
    /// Provider that supplied the message.
    provider_id: ProviderId,
    /// Bus-local subscription identifier.
    subscription_id: Id,
    /// Logical subscriber identifier.
    subscriber_id: SubscriberId,
    /// One-based attempt number assigned by the facade.
    retry_attempt: u32,
    /// Provider attempt number, when reported by the backend.
    provider_attempt: Option<u32>,
    /// Non-sensitive metadata supplied by the provider.
    provider_metadata: ProviderMessageMetadata,
    /// Whether the provider supports settling this message.
    can_settle: bool,
    /// Whether the message was forwarded as a dead letter.
    dead_letter: bool,
}

impl DeliveryContext {
    /// Creates a context supplied by the facade after receiving an event.
    ///
    /// # Parameters
    /// - `provider_id`: Provider that delivered the event.
    /// - `subscription_id`: Bus-local identifier for the subscription.
    /// - `subscriber_id`: Logical identifier for the subscriber.
    ///
    /// # Returns
    /// A context with the initial retry attempt and no provider metadata.
    #[must_use]
    #[inline]
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
    ///
    /// # Parameters
    /// - `value`: One-based retry attempt number.
    ///
    /// # Returns
    /// The updated context.
    #[must_use]
    #[inline]
    pub fn with_retry_attempt(mut self, value: u32) -> Self {
        self.retry_attempt = value;
        self
    }

    /// Sets the provider attempt number, when the provider supplies one.
    ///
    /// # Parameters
    /// - `value`: Provider-reported attempt number.
    ///
    /// # Returns
    /// The updated context.
    #[must_use]
    #[inline]
    pub fn with_provider_attempt(mut self, value: u32) -> Self {
        self.provider_attempt = Some(value);
        self
    }

    /// Replaces non-sensitive provider message metadata.
    ///
    /// # Parameters
    /// - `value`: Metadata reported by the provider.
    ///
    /// # Returns
    /// The updated context.
    #[must_use]
    #[inline]
    pub fn with_provider_metadata(mut self, value: ProviderMessageMetadata) -> Self {
        self.provider_metadata = value;
        self
    }

    /// Records whether the provider supports settling this delivery.
    ///
    /// # Parameters
    /// - `value`: Whether ACK/NACK settlement is supported.
    ///
    /// # Returns
    /// The updated context.
    #[must_use]
    #[inline]
    pub fn with_settlement(mut self, value: bool) -> Self {
        self.can_settle = value;
        self
    }

    /// Marks this delivery as a dead letter.
    ///
    /// # Returns
    /// The updated context.
    #[must_use]
    #[inline]
    pub fn as_dead_letter(mut self) -> Self {
        self.dead_letter = true;
        self
    }

    /// Returns the source provider ID.
    ///
    /// # Returns
    /// The provider identifier that received the event.
    #[must_use]
    #[inline]
    pub fn provider_id(&self) -> &ProviderId {
        &self.provider_id
    }

    /// Returns the bus-local subscription object ID.
    ///
    /// # Returns
    /// The identifier assigned to this subscription by its bus.
    #[must_use = "the subscription ID identifies this bus-local subscription"]
    #[inline]
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }

    /// Returns the logical subscriber ID.
    ///
    /// # Returns
    /// The stable identifier for the logical consumer.
    #[must_use = "the subscriber ID identifies the logical consumer"]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns the facade retry attempt, starting at one.
    ///
    /// # Returns
    /// The one-based facade attempt count.
    #[must_use]
    #[inline]
    pub fn retry_attempt(&self) -> u32 {
        self.retry_attempt
    }

    /// Returns the provider attempt, or `None` when unavailable.
    ///
    /// # Returns
    /// The provider-reported attempt count, when supplied.
    #[must_use]
    #[inline]
    pub fn provider_attempt(&self) -> Option<u32> {
        self.provider_attempt
    }

    /// Returns provider-supplied non-sensitive metadata.
    ///
    /// # Returns
    /// The metadata copied from the provider message.
    #[must_use]
    #[inline]
    pub fn provider_metadata(&self) -> &ProviderMessageMetadata {
        &self.provider_metadata
    }

    /// Returns whether the provider can settle this delivery.
    ///
    /// # Returns
    /// `true` if the provider supports settlement for this delivery.
    #[must_use]
    #[inline]
    pub fn can_settle(&self) -> bool {
        self.can_settle
    }

    /// Returns whether this delivery is a dead letter.
    ///
    /// # Returns
    /// `true` if this delivery was forwarded as a dead letter.
    #[must_use]
    #[inline]
    pub fn is_dead_letter(&self) -> bool {
        self.dead_letter
    }
}
