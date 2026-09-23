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
    #[error("provider {provider_id} failed {operation} (invalid_settlement_token/{reason}): {source}")]
    InvalidSettlementToken {
        /// Provider that produced the token.
        provider_id: Box<str>,
        /// Operation that attempted to use the token.
        operation: &'static str,
        /// Subscription context, when available.
        resource: Option<Box<str>>,
        /// Stable rejection reason.
        reason: &'static str,
        /// Whether a retry is known to be appropriate.
        retryable: Option<bool>,
        /// Original provider failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
}

impl SpiError {
    /// Returns the provider ID retained by this SPI failure.
    #[must_use]
    pub fn provider_id(&self) -> &str {
        match self {
            Self::Operation { provider_id, .. } | Self::InvalidSettlementToken { provider_id, .. } => provider_id,
        }
    }

    /// Returns the operation that failed.
    #[must_use]
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Operation { operation, .. } | Self::InvalidSettlementToken { operation, .. } => operation,
        }
    }

    /// Returns `Some` topic or subscription context when known, or `None`.
    #[must_use]
    pub fn resource(&self) -> Option<&str> {
        match self {
            Self::Operation { resource, .. } | Self::InvalidSettlementToken { resource, .. } => resource.as_deref(),
        }
    }

    /// Returns the stable error classification.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Operation { kind, .. } => kind,
            Self::InvalidSettlementToken { .. } => "invalid_settlement_token",
        }
    }

    /// Returns `Some(true)` for known retryable errors, `Some(false)` for
    /// known terminal errors, or `None` when the provider cannot classify it.
    #[must_use]
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Operation { retryable, .. } | Self::InvalidSettlementToken { retryable, .. } => *retryable,
        }
    }
}
