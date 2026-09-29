// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Structured terminal subscription causes retained for recovery.

use std::sync::Arc;

use crate::error::CodecError;
use crate::error::SpiError;
use crate::model::EventId;

/// Why new receives stopped; durable source deliveries remain unsettled.
///
/// The handle retains the first terminal cause in a shared owner. Restoring
/// compatibility or correcting the provider requires creating a new
/// subscription.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_event_bus::error::CodecError;
/// use qubit_event_bus::error::ReceiveError;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::SubscriptionStopReason;
///
/// let reason = Arc::new(SubscriptionStopReason::Codec {
///     event_id: EventId::new("received-event").unwrap(),
///     error: Arc::new(CodecError::NativeTypeMismatch),
/// });
/// let stopped = ReceiveError::Stopped(Arc::clone(&reason));
/// if let ReceiveError::Stopped(actual) = stopped {
///     assert!(Arc::ptr_eq(&actual, &reason));
/// }
/// ```
#[derive(Clone, Debug, thiserror::Error)]
pub enum SubscriptionStopReason {
    /// Codec or payload boundary failure after event identity was obtained.
    #[error("subscription stopped at event {event_id:?}: {error}")]
    Codec {
        /// Identity of the event that stopped receiving.
        event_id: EventId,
        /// Original codec failure with its source chain.
        #[source]
        error: Arc<CodecError>,
    },
    /// Provider receive failure, potentially before trustworthy event identity.
    #[error("subscription stopped at provider boundary: {error}")]
    Provider {
        /// Original provider failure with its source chain.
        #[source]
        error: Arc<SpiError>,
    },
}
