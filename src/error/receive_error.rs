// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiving failures.

use qubit_clock::TimeError;

use crate::error::CodecError;
use crate::error::SpiError;
use crate::model::EventId;

/// A subscription could not receive or decode its next delivery.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReceiveError {
    /// The backend could not receive an event.
    #[error(transparent)]
    Spi(#[from] SpiError),
    /// An inbound payload could not be decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// The injected timer failed while backing off a provider settlement retry.
    #[error("event bus timer failed while retrying settlement: {0}")]
    Timer(#[from] TimeError),
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
