// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One failed provider subscription close.

use std::error::Error;
use std::fmt;

use crate::error::SpiError;
use crate::model::SubscriberId;

/// One subscription close failure with its logical subscriber identity.
///
/// The value retains the provider error, including its source chain, and is
/// exposed through the shutdown error snapshot for inspection.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SubscriptionCloseFailure;
/// use qubit_event_bus::model::SubscriberId;
///
/// fn failed_subscriber(failure: &SubscriptionCloseFailure) -> &SubscriberId {
///     failure.subscriber_id()
/// }
/// ```
#[derive(Debug)]
#[must_use]
pub struct SubscriptionCloseFailure {
    /// Logical subscriber whose provider-side subscription failed to close.
    subscriber_id: SubscriberId,
    /// Provider error and its preserved source chain.
    error: SpiError,
}

impl SubscriptionCloseFailure {
    /// Creates one ledger entry for a failed subscription close.
    ///
    /// # Parameters
    /// - `subscriber_id`: logical identity associated with the failed close.
    /// - `error`: source-preserving provider failure.
    ///
    /// # Returns
    /// A failure record that retains both values for later shutdown reporting.
    pub(crate) fn new(subscriber_id: SubscriberId, error: SpiError) -> Self {
        Self { subscriber_id, error }
    }

    /// Returns the logical subscriber whose provider subscription failed to
    /// close.
    ///
    /// # Returns
    /// The subscriber ID borrowed from this failure record.
    #[must_use = "Use the returned subscriber id."]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns the source-preserving provider error.
    ///
    /// # Returns
    /// The provider error borrowed from this failure record.
    #[must_use = "Use the returned error."]
    #[inline]
    pub fn error(&self) -> &SpiError {
        &self.error
    }
}

impl fmt::Display for SubscriptionCloseFailure {
    /// Formats the failed subscriber and its provider error.
    ///
    /// # Returns
    /// The formatted subscriber identity and provider error.
    ///
    /// # Errors
    /// Returns the formatter's error if writing either value fails.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "subscription {}: {}",
            self.subscriber_id.as_str(),
            self.error
        )
    }
}

impl Error for SubscriptionCloseFailure {
    /// Exposes the original provider close error as the source.
    ///
    /// # Returns
    /// `Some` containing the provider error retained by this failure record.
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}
