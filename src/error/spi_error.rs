// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Backend SPI failures with provider and operation context.

use std::error::Error;

/// An error returned across the provider SPI boundary.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SpiError {
    /// A provider operation failed and retained its original error.
    #[error("provider {provider_id} failed {operation} ({kind}): {source}")]
    Operation {
        /// Provider that produced the error.
        provider_id: Box<str>,
        /// Operation being performed.
        operation: &'static str,
        /// Topic or subscription context, when available.
        resource: Option<Box<str>>,
        /// Stable error classification.
        kind: &'static str,
        /// Whether a retry is known to be appropriate.
        retryable: Option<bool>,
        /// Original provider failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
    /// A settlement token was rejected by its owning provider.
    #[error("provider {provider_id} rejected a settlement token: {reason}")]
    InvalidSettlementToken {
        /// Provider that produced the token.
        provider_id: Box<str>,
        /// Stable rejection reason.
        reason: &'static str,
    },
}
