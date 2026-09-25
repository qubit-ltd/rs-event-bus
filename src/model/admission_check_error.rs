// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors raised when a receipt fails a requested admission condition.

/// Why a receipt does not meet an admission requirement.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum AdmissionCheckError {
    /// The provider accepted the message without reporting individual
    /// destinations.
    #[error("destination admission visibility is unavailable")]
    VisibilityUnavailable,
    /// A publisher interceptor stopped dispatch before provider admission.
    #[error("publication was dropped by an interceptor")]
    Dropped,
    /// No reported destination accepted the event.
    #[error("no destination accepted the publication")]
    NoAcceptedDestination,
    /// Some destinations rejected admission despite at least one acceptance.
    #[error("{count} destinations rejected the publication")]
    RejectedDestinations {
        /// Number of destinations that rejected admission.
        count: usize,
    },
}
