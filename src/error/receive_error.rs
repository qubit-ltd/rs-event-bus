// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiving failures.

use std::sync::Arc;

use qubit_clock::TimeError;

use crate::error::CodecError;
use crate::error::SpiError;
use crate::model::EventId;
use crate::model::SubscriptionStopReason;

/// A subscription could not receive or decode its next delivery.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::ReceiveError;
///
/// let error = ReceiveError::Closed;
/// assert!(matches!(error, ReceiveError::Closed));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum ReceiveError {
    /// The backend could not receive an event.
    #[error(transparent)]
    Spi(
        /// Failure returned by the provider receive operation.
        #[from]
        SpiError,
    ),
    /// An inbound payload could not be decoded.
    #[error(transparent)]
    Codec(
        /// Failure while decoding the received payload.
        #[from]
        CodecError,
    ),
    /// The injected timer failed while backing off a provider settlement retry.
    #[error("event bus timer failed while retrying settlement: {0}")]
    Timer(
        /// Failure from the injected clock or timer.
        #[from]
        TimeError,
    ),
    /// New receives stopped with a stable cause retained for recovery.
    #[error("{0}")]
    Stopped(#[source] Arc<SubscriptionStopReason>),
    /// The subscription has closed.
    #[error("cannot receive after subscription close")]
    Closed,
    /// Dead-letter publication exhausted its configured retry budget; the
    /// original provider token was left unsettled for recovery.
    #[error("dead-letter forwarding failed for event {event_id:?}: {message}")]
    DeadLetterForwardFailed {
        /// Source event whose dead-letter envelope could not be admitted.
        event_id: EventId,
        /// Last forwarding or admission failure.
        message: Box<str>,
    },
}
