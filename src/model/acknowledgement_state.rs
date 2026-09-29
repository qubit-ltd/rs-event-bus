// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal state of a delivery acknowledgement handle.

/// The terminal ACK/NACK decision for one delivery.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AcknowledgementState;
///
/// let state = AcknowledgementState::Pending;
/// assert_eq!(state, AcknowledgementState::Pending);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum AcknowledgementState {
    /// Neither decision has been made.
    Pending,
    /// The handler acknowledged the delivery.
    Acknowledged,
    /// The handler negatively acknowledged the delivery.
    NegativelyAcknowledged,
}
