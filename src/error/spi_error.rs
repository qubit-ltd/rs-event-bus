// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Backend SPI failures with provider and operation context.

use std::error::Error;

use crate::model::PublishEffect;

/// An error returned across the provider SPI boundary.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SpiError;
///
/// let error = SpiError::Operation {
///     provider_id: "memory".into(),
///     operation: "publish",
///     resource: None,
///     kind: "unavailable",
///     retryable: Some(true),
///     source: Box::new(std::io::Error::other("offline")),
/// };
/// assert_eq!(error.provider_id(), "memory");
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
#[must_use]
pub enum SpiError {
    /// A publish operation failed with explicit admission evidence.
    #[error("provider {provider_id} failed publish ({kind}, {effect:?}): {source}")]
    Publish {
        /// Provider that produced the failure.
        provider_id: Box<str>,
        /// Topic context, when available.
        resource: Option<Box<str>>,
        /// Stable classification without secret provider data.
        kind: &'static str,
        /// Whether another attempt is known to be appropriate.
        retryable: Option<bool>,
        /// External admission evidence for this attempt.
        effect: PublishEffect,
        /// Original provider failure.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },
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
    /// Returns explicit publish evidence or conservative uncertainty for
    /// generic failures. A generic Operation cannot prove that publishing
    /// had no external effect.
    #[must_use]
    #[inline]
    pub fn publish_effect(&self) -> PublishEffect {
        match self {
            Self::Publish { effect, .. } => *effect,
            _ => PublishEffect::MayHaveBeenAccepted,
        }
    }

    /// Returns the provider ID retained by this SPI failure.
    ///
    /// The returned string is borrowed from `self` and remains valid only
    /// while `self` is borrowed. This lookup does not allocate.
    ///
    /// # Returns
    /// The provider identifier associated with the failure.
    #[must_use]
    #[inline]
    pub fn provider_id(&self) -> &str {
        match self {
            Self::Publish { provider_id, .. }
            | Self::Operation { provider_id, .. }
            | Self::InvalidSettlementToken { provider_id, .. } => provider_id,
        }
    }

    /// Returns the operation that failed.
    ///
    /// # Returns
    /// The static operation name.
    #[must_use]
    #[inline]
    pub fn operation(&self) -> &'static str {
        match self {
            Self::Publish { .. } => "publish",
            Self::Operation { operation, .. } | Self::InvalidSettlementToken { operation, .. } => operation,
        }
    }

    /// Returns `Some` topic or subscription context when known, or `None`.
    ///
    /// Any returned string is borrowed from `self` and remains valid only
    /// while `self` is borrowed. This lookup does not allocate.
    ///
    /// # Returns
    /// The associated resource name, if the operation had one.
    #[must_use]
    #[inline]
    pub fn resource(&self) -> Option<&str> {
        match self {
            Self::Publish { resource, .. }
            | Self::Operation { resource, .. }
            | Self::InvalidSettlementToken { resource, .. } => resource.as_deref(),
        }
    }

    /// Returns the stable error classification.
    ///
    /// # Returns
    /// The stable classification string.
    #[must_use]
    #[inline]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Publish { kind, .. } | Self::Operation { kind, .. } => kind,
            Self::InvalidSettlementToken { .. } => "invalid_settlement_token",
        }
    }

    /// Returns `Some(true)` for known retryable errors, `Some(false)` for
    /// known terminal errors, or `None` when the provider cannot classify it.
    ///
    /// # Returns
    /// The provider's retry classification when available.
    #[must_use]
    #[inline]
    pub fn retryable(&self) -> Option<bool> {
        match self {
            Self::Publish { retryable, .. }
            | Self::Operation { retryable, .. }
            | Self::InvalidSettlementToken { retryable, .. } => *retryable,
        }
    }
}
