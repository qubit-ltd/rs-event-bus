// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Direction of an encoded payload size check.

/// Distinguishes publishing and receiving boundary limits.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::CodecError;
/// use qubit_event_bus::model::PayloadDirection;
///
/// let error = CodecError::PayloadTooLarge {
///     direction: PayloadDirection::Receive,
///     actual: 1_025,
///     limit: 1_024,
/// };
/// assert!(matches!(error, CodecError::PayloadTooLarge {
///     direction: PayloadDirection::Receive, ..
/// }));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadDirection {
    /// Bytes produced before provider publication.
    Publish,
    /// Bytes received before any codec callback.
    Receive,
}
