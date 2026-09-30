// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Evidence about the external effect of a failed publication.

/// Whether provider admission can be ruled out after a failed attempt.
///
/// A missing acknowledgement does not prove `NotAccepted`. Across a logical
/// publication, any uncertain failed attempt keeps the final effect uncertain.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::PublishError;
/// use qubit_event_bus::PublishFailure;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::PublishEffect;
///
/// let failure = PublishFailure::new(
///     EventId::new("closed-publication").unwrap(),
///     PublishEffect::NotAccepted,
///     PublishError::Closed,
/// );
/// assert_eq!(failure.effect(), PublishEffect::NotAccepted);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum PublishEffect {
    /// Evidence proves that the event was not admitted.
    NotAccepted,
    /// Admission occurred or cannot be ruled out.
    MayHaveBeenAccepted,
}
