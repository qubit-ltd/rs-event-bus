// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Message acknowledgement modes.

/// Controls how subscriber handlers acknowledge event processing.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::AckMode;
///
/// assert_eq!(AckMode::default(), AckMode::Auto);
/// assert_eq!(AckMode::Manual, AckMode::Manual);
/// ```
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Hash)]
pub enum AckMode {
    /// A handler success automatically acknowledges the event.
    #[default]
    Auto,
    /// Handler code receives an [`crate::Acknowledgement`] and must ACK or
    /// NACK before returning. A successful return without either decision
    /// is treated as a handler failure and may be retried.
    Manual,
}
