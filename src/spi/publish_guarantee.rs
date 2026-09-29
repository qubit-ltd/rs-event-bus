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
    /// No provider acknowledgement is guaranteed.
    FireAndForget,
    /// The provider accepted the message.
    Accepted,
    /// The provider confirmed publication.
    Confirmed,
    /// The message was durably stored.
    DurablyStored,
}
