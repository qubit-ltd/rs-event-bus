// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcome observed after one queued notification reaches the publisher worker.

use crate::error::EventIdGenerationError;
use crate::error::PublishError;
use crate::model::PublishReceipt;

/// The result of constructing or publishing one queued notification.
///
/// A `Published` receipt reports provider admission only. It does not imply
/// that a subscriber handler completed.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::NotificationOutcome;
/// use qubit_event_bus::error::PublishError;
///
/// let outcome = NotificationOutcome::PublishFailed(PublishError::Closed);
/// assert!(matches!(outcome, NotificationOutcome::PublishFailed(_)));
/// ```
#[non_exhaustive]
#[derive(Debug)]
#[must_use]
pub enum NotificationOutcome {
    /// The provider returned an admission receipt.
    Published(
        /// Provider admission result, without a handler completion guarantee.
        PublishReceipt,
    ),
    /// The facade or provider rejected the publication.
    PublishFailed(
        /// Facade or provider failure encountered while publishing the request.
        PublishError,
    ),
    /// A request could not be built because event identity generation failed.
    RequestFailed(
        /// Recoverable failure from the event identifier generator.
        EventIdGenerationError,
    ),
}
