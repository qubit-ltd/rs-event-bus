// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Conflicting delivery acknowledgement decisions.

/// A conflicting acknowledgement decision.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AcknowledgementError;
///
/// let error = AcknowledgementError::AlreadyCompleted;
/// assert!(matches!(error, AcknowledgementError::AlreadyCompleted));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum AcknowledgementError {
    /// A different terminal decision has already completed this delivery.
    #[error("acknowledgement already completed with a conflicting decision")]
    AlreadyCompleted,
}
