// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcome observed after one queued notification reaches the publisher worker.

use crate::error::EventIdGenerationError;
use crate::error::PublishFailure;
use crate::model::PublishReceipt;

/// The result of constructing or publishing one queued notification.
///
/// `Published` contains the successful receipt returned by the publishing
/// facade. A successful receipt can report an interceptor drop or a send
/// without admission confirmation; inspect
/// [`PublishReceipt::admission_outcome()`] to classify it. No outcome implies
/// that a subscriber handler completed.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::NotificationOutcome;
/// use qubit_event_bus::error::PublishError;
///
/// let outcome = NotificationOutcome::PublishFailed(qubit_event_bus::PublishFailure::new(
///     qubit_event_bus::model::EventId::new("notification").unwrap(),
///     qubit_event_bus::model::PublishEffect::NotAccepted, PublishError::Closed));
/// assert!(matches!(outcome, NotificationOutcome::PublishFailed(_)));
/// ```
#[non_exhaustive]
#[derive(Debug)]
#[must_use]
pub enum NotificationOutcome {
    /// The publishing facade returned a successful receipt.
    Published(
        /// The successful facade receipt, including interceptor drops and
        /// sends without admission confirmation. Use
        /// [`PublishReceipt::admission_outcome()`] to inspect its
        /// classification.
        PublishReceipt,
    ),
    /// Publication failed; inspect [`PublishFailure::effect()`] before deciding
    /// whether to retry. A failed call does not necessarily mean the provider
    /// rejected or failed to apply the publication.
    PublishFailed(
        /// Facade or provider failure encountered while publishing the request.
        PublishFailure,
    ),
    /// A request could not be built because event identity generation failed.
    RequestFailed(
        /// Recoverable failure from the event identifier generator.
        EventIdGenerationError,
    ),
}
