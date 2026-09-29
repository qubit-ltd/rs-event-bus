// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Requested behavior unsupported by the selected backend.

/// An operation requires a capability the selected backend does not provide.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::CapabilityError;
///
/// let error = CapabilityError::Unsupported { capability: "ordering" };
/// assert!(matches!(error, CapabilityError::Unsupported { capability: "ordering" }));
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum CapabilityError {
    /// The backend requires an encoded payload, but the topic has no codec.
    #[error("a codec is required for this topic")]
    CodecRequired,
    /// The selected backend does not support the named capability.
    #[error("unsupported event bus capability: {capability}")]
    Unsupported {
        /// Stable capability name.
        capability: &'static str,
    },
}
