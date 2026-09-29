// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Final publication failure retaining identity, effect and cause.

use crate::error::PublishError;
use crate::model::EventId;
use crate::model::PublishEffect;

/// A failed logical publication whose external admission may be uncertain.
///
/// The event identity stays stable across interception and retry attempts.
/// The effect describes the whole logical publication, while `cause` retains
/// the final error and its original source chain.
///
/// # Examples
///
/// ```
/// use std::error::Error;
/// use qubit_event_bus::PublishError;
/// use qubit_event_bus::PublishFailure;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::PublishEffect;
///
/// let failure = PublishFailure::new(
///     EventId::new("order-42").unwrap(),
///     PublishEffect::NotAccepted,
///     PublishError::Closed,
/// );
/// assert_eq!(failure.event_id().as_str(), "order-42");
/// assert_eq!(failure.effect(), PublishEffect::NotAccepted);
/// assert!(failure.source().unwrap().is::<PublishError>());
/// ```
#[derive(Debug, thiserror::Error)]
#[error("publication {event_id:?} failed ({effect:?}): {cause}")]
pub struct PublishFailure {
    /// Original caller-provided identity of the failed publication.
    event_id: EventId,
    /// Aggregate evidence of external admission across all failed attempts.
    effect: PublishEffect,
    /// Final operation error preserved as the standard error source.
    #[source]
    cause: PublishError,
}

impl PublishFailure {
    /// Creates a final failure with the original requested identity and cause.
    ///
    /// # Parameters
    /// - `event_id`: original identity of the logical publication.
    /// - `effect`: aggregate admission evidence across its attempts.
    /// - `cause`: final publication error retained without changing its source.
    ///
    /// # Returns
    /// A failure exposing stable identity, effect and the original cause.
    #[must_use]
    pub fn new(event_id: EventId, effect: PublishEffect, cause: PublishError) -> Self {
        Self {
            event_id,
            effect,
            cause,
        }
    }
    /// Returns the original event identity supplied by the caller.
    ///
    /// # Returns
    /// A borrowed identity that is stable across publication attempts.
    #[must_use = "the event ID identifies the original logical publication"]
    #[inline]
    pub fn event_id(&self) -> &EventId {
        &self.event_id
    }
    /// Returns admission evidence aggregated for the logical publication.
    ///
    /// # Returns
    /// Whether external admission can be ruled out for the whole publication.
    #[must_use]
    #[inline]
    pub fn effect(&self) -> PublishEffect {
        self.effect
    }
    /// Returns the final error without discarding its original source chain.
    ///
    /// # Returns
    /// The borrowed terminal cause, also available through `Error::source`.
    #[must_use = "the cause retains the final publication error"]
    #[inline]
    pub fn cause(&self) -> &PublishError {
        &self.cause
    }
    /// Consumes the wrapper and returns its original final error.
    ///
    /// # Returns
    /// The owned cause; the wrapper identity and effect are no longer retained.
    #[must_use = "the owned cause must be handled after consuming the failure"]
    #[inline]
    pub fn into_cause(self) -> PublishError {
        self.cause
    }
}
