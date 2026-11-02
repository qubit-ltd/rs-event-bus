// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider publish acknowledgement guarantees.

/// Strongest guarantee represented by successful provider publication.
///
/// The levels are cumulative: `Accepted` includes `FireAndForget`, `Confirmed`
/// includes `Accepted`, and `DurablyStored` includes `Confirmed`. The provider
/// must document its admission, confirmation, and storage boundaries and the
/// failures those boundaries cover. None of these guarantees means that a
/// subscriber handler completed successfully.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::PublishGuarantee;
///
/// let guarantee = PublishGuarantee::Accepted;
/// assert_eq!(guarantee, PublishGuarantee::Accepted);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublishGuarantee {
    /// The provider completed its send or commit attempt without promising a
    /// positive admission acknowledgement.
    FireAndForget,
    /// The provider accepted the message at its declared admission boundary.
    /// That boundary may be an in-memory provider queue; this does not imply
    /// transport confirmation, durable storage, or consumer completion.
    Accepted,
    /// The receiving transport returned a positive confirmation that the
    /// provider observed. This includes `Accepted` but does not by itself imply
    /// durable storage or consumer completion.
    Confirmed,
    /// The message reached the provider's declared durable storage boundary.
    /// The provider must document the recoverable failure model and required
    /// storage configuration. This does not imply exactly-once delivery,
    /// disaster recovery across all replicas, or consumer completion.
    DurablyStored,
}
