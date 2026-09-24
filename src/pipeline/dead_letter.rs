// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Standard dead-letter record construction, kept separate from transport.

use crate::error::ConfigurationError;
use crate::error::DeliveryError;
use crate::error::EventIdGenerationError;
use crate::model::DEAD_LETTER_HEADER;
use crate::model::DEAD_LETTER_HEADER_VALUE;
use crate::model::DeadLetterEvent;
use crate::model::Delivery;
use crate::model::EventEnvelope;
use crate::model::Topic;

/// Failure while validating or creating the standard dead-letter event.
#[derive(Debug, thiserror::Error)]
pub(crate) enum DeadLetterBuildError {
    /// The configured dead-letter topic is invalid.
    #[error(transparent)]
    Configuration(#[from] ConfigurationError),
    /// A new event identifier could not be generated.
    #[error(transparent)]
    EventId(#[from] EventIdGenerationError),
}

/// Builds a dead-letter record without mutating or cloning the original event.
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
    let mut envelope = EventEnvelope::new(topic, record)?;
    envelope.set_system_header(DEAD_LETTER_HEADER, DEAD_LETTER_HEADER_VALUE);
    Ok(Some(envelope))
}
