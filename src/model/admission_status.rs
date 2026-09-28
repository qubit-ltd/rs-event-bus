// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One destination's provider-reported admission decision.

/// Admission outcome for a single destination, before handler execution.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionStatus;
///
/// let status = AdmissionStatus::Rejected("queue full".into());
/// assert!(matches!(status, AdmissionStatus::Rejected(_)));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum AdmissionStatus {
    /// The provider accepted the delivery for dispatch.
    Accepted,
    /// A subscription filter excluded the event.
    Filtered,
    /// The provider rejected admission for the stated reason.
    Rejected(
        /// Provider-supplied explanation for the rejection.
        Box<str>,
    ),
}
