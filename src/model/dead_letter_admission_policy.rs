// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission evidence required before a source delivery is settled as
//! forwarded.

/// Evidence required to treat a dead-letter publication as accepted.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::DeadLetterAdmissionPolicy;
///
/// let policy = DeadLetterAdmissionPolicy::KnownDestination;
/// assert_eq!(policy, DeadLetterAdmissionPolicy::KnownDestination);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum DeadLetterAdmissionPolicy {
    /// Accept provider-level opaque acknowledgement when its guarantee is at
    /// least `Accepted`; known partial admission also counts as forwarded.
    /// A zero-destination or dropped publication never counts as accepted.
    TransportAccepted,
    /// Require at least one reported destination to accept admission.
    KnownDestination,
}
