// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Handler acknowledgement modes.

/// How handler completion is acknowledged.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AckMode;
///
/// let mode = AckMode::Manual;
/// assert_eq!(mode, AckMode::Manual);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum AckMode {
    /// Handler success automatically accepts the delivery.
    Auto,
    /// Handler must explicitly ACK and return success.
    Manual,
}
