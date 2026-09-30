// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Standard dead-letter record construction, kept separate from transport.

use std::sync::Arc;

use super::DeadLetterBuildError;
use crate::error::DeliveryError;
use crate::model::DEAD_LETTER_HEADER;
use crate::model::DEAD_LETTER_HEADER_VALUE;
use crate::model::DeadLetterEvent;
use crate::model::Delivery;
use crate::model::EventEnvelope;
use crate::model::EventId;
use crate::model::SubscriberId;
use crate::model::Topic;

/// Builds a dead-letter record without mutating or cloning the original event.
///
/// # Type Parameters
/// - `T`: payload type retained by the original delivery.
///
/// # Parameters
/// - `delivery`: Failed delivery whose event and subscriber identify the
///   record.
/// - `error`: Terminal processing failure summarized in the dead-letter record.
/// - `topic`: Destination for the dead-letter event.
///
/// # Returns
/// The constructed envelope, or `None` when the delivery is already a dead
/// letter.
///
/// # Errors
/// Returns an error when `topic` is not a valid topic name.
pub(crate) fn dead_letter_envelope<T: 'static>(
    delivery: &Delivery<T>,
    error: &DeliveryError,
    topic: &str,
) -> Result<Option<EventEnvelope<DeadLetterEvent<T>>>, DeadLetterBuildError> {
    if delivery.context().is_dead_letter() {
        return Ok(None);
    }
    let record = DeadLetterEvent::new(
        delivery.event_arc(),
        delivery.context().subscriber_id().clone(),
        error.to_string().into_boxed_str(),
    );
    let topic = Topic::<DeadLetterEvent<T>>::new(topic)?;
    let id = dead_letter_id(delivery.event().id(), delivery.context().subscriber_id());
    let mut envelope = EventEnvelope::with_id_and_shared_payload(topic, Arc::new(record), id);
    envelope.set_system_header(DEAD_LETTER_HEADER, DEAD_LETTER_HEADER_VALUE);
    Ok(Some(envelope))
}

/// Derives a stable, domain-separated identifier for the original event and
/// logical subscriber. This supports deduplication but does not provide
/// exactly-once delivery; providers may accept a publish whose response is
/// lost.
///
/// # Parameters
/// - `event_id`: identity of the original event.
/// - `subscriber_id`: logical subscriber that handled the event.
///
/// # Returns
/// A stable event ID scoped to the original event and subscriber.
///
/// # Panics
///
/// Panics only if the generated portable ID violates `EventId` validation.
fn dead_letter_id(event_id: &EventId, subscriber_id: &SubscriberId) -> EventId {
    /// Computes a seeded hash over two identifiers with a separator.
    ///
    /// # Parameters
    /// - `seed`: initial hash state.
    /// - `first`: first identifier component.
    /// - `second`: second identifier component.
    ///
    /// # Returns
    /// The 64-bit hash of the ordered components.
    #[inline]
    fn hash(seed: u64, first: &str, second: &str) -> u64 {
        let mut value = seed;
        for byte in first.bytes().chain([0]).chain(second.bytes()) {
            value ^= u64::from(byte);
            value = value.wrapping_mul(0x100_0000_01b3);
        }
        value
    }
    let first = hash(0xcbf2_9ce4_8422_2325, event_id.as_str(), subscriber_id.as_str());
    let second = hash(0x8422_2325_cbf2_9ce4, subscriber_id.as_str(), event_id.as_str());
    EventId::new(format!("dlq-{first:016x}{second:016x}")).expect("derived ID is portable")
}

#[cfg(test)]
mod tests {
    use super::dead_letter_id;
    use crate::model::EventId;
    use crate::model::SubscriberId;

    #[test]
    fn test_dead_letter_id_is_stable_and_scoped_to_subscriber() {
        let event_id = EventId::new("order-42").unwrap();
        let subscriber = SubscriberId::new("billing").unwrap();
        assert_eq!(
            dead_letter_id(&event_id, &subscriber).as_str(),
            "dlq-5177e8019aa006974ef10db127dff43c"
        );
        assert_ne!(
            dead_letter_id(&event_id, &subscriber),
            dead_letter_id(&event_id, &SubscriberId::new("audit").unwrap())
        );
    }
}
