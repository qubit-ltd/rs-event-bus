// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregated failures from closing facade-managed subscriptions.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::SubscriptionCloseFailure;

/// A stable, shareable collection of subscription close failures.
///
/// The collection is produced by facade shutdown and preserves each provider
/// error as an inspectable [`SubscriptionCloseFailure`]. Its iterator borrows
/// the snapshot, so examining failures does not clone error sources.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SubscriptionCloseErrors;
///
/// fn failed_close_count(errors: &SubscriptionCloseErrors) -> usize {
///     errors.iter().count()
/// }
/// ```
#[derive(Debug)]
#[must_use]
pub struct SubscriptionCloseErrors {
    /// Shared close-failure records captured by the bus lifecycle ledger.
    failures: Vec<Arc<SubscriptionCloseFailure>>,
}

impl SubscriptionCloseErrors {
    /// Builds an immutable close-error snapshot from the bus lifecycle ledger.
    ///
    /// # Parameters
    /// - `failures`: shared failure records retained by the lifecycle ledger.
    ///
    /// # Returns
    /// An immutable collection over the supplied records.
    #[inline]
    pub(crate) fn from_failures(failures: Vec<Arc<SubscriptionCloseFailure>>) -> Self {
        Self { failures }
    }

    /// Returns the number of failed subscription closes.
    ///
    /// # Returns
    /// The number of records in this immutable snapshot.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.failures.len()
    }

    /// Returns whether no subscription close failed.
    ///
    /// # Returns
    /// `true` when the snapshot contains no close failures.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    /// Iterates over every failed subscription and its original provider error.
    ///
    /// # Returns
    /// A borrowing iterator in the order failures were recorded.
    #[must_use = "Use the returned iter."]
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &SubscriptionCloseFailure> {
        self.failures.iter().map(Arc::as_ref)
    }
}

impl fmt::Display for SubscriptionCloseErrors {
    /// Formats the total failure count and every affected subscriber.
    ///
    /// # Parameters
    /// - `formatter`: Destination that receives the failure summary.
    ///
    /// # Returns
    /// `Ok(())` when the summary is written successfully.
    ///
    /// # Errors
    /// Returns `fmt::Error` if the formatter cannot accept the summary.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} subscription close failure(s)", self.len())?;
        for failure in &self.failures {
            write!(formatter, "; {failure}")?;
        }
        Ok(())
    }
}

impl Error for SubscriptionCloseErrors {
    /// Exposes the first close failure while callers can inspect all via
    /// [`Self::iter`].
    ///
    /// # Returns
    /// The first recorded close failure, or `None` when this snapshot is empty.
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(Arc::as_ref)
            .map(|failure| failure as &(dyn Error + 'static))
    }
}
