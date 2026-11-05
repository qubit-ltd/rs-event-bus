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
/// The collection is produced by facade shutdown. It retains at most 256
/// inspectable [`SubscriptionCloseFailure`] samples; use [`Self::total_count`]
/// for the complete observed failure count and [`Self::omitted_count`] for
/// failures without retained samples. The sample limit counts records, not
/// bytes. The total count saturates at `u64::MAX`.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::error::SubscriptionCloseErrors;
///
/// fn close_failure_summary(errors: &SubscriptionCloseErrors) -> (u64, u64, bool) {
///     (errors.total_count(), errors.omitted_count(), errors.is_truncated())
/// }
/// ```
#[derive(Debug)]
#[must_use]
pub struct SubscriptionCloseErrors {
    /// Shared close-failure records captured by the bus lifecycle ledger.
    failures: Vec<Arc<SubscriptionCloseFailure>>,
    /// Total number of distinct subscription close failures observed.
    total_count: u64,
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
        let total_count = u64::try_from(failures.len()).unwrap_or(u64::MAX);
        Self { failures, total_count }
    }

    /// Builds an immutable snapshot from bounded samples and a cumulative
    /// count.
    ///
    /// # Parameters
    /// - `failures`: retained close-failure samples.
    /// - `total_count`: total number of close failures observed.
    ///
    /// # Returns
    /// An immutable collection over the supplied samples and total count.
    #[inline]
    pub(crate) fn from_summary(failures: Vec<Arc<SubscriptionCloseFailure>>, total_count: u64) -> Self {
        Self { failures, total_count }
    }

    /// Returns the number of retained close-failure samples.
    ///
    /// # Returns
    /// The number of records retained in this immutable snapshot.
    #[must_use]
    #[inline]
    pub fn len(&self) -> usize {
        self.failures.len()
    }

    /// Returns the total number of failed subscription closes observed.
    #[must_use]
    #[inline]
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Returns the number of failures omitted from this bounded snapshot.
    #[must_use]
    #[inline]
    pub fn omitted_count(&self) -> u64 {
        self.total_count
            .saturating_sub(u64::try_from(self.failures.len()).unwrap_or(u64::MAX))
    }

    /// Returns whether this snapshot omits one or more failure records.
    #[must_use]
    #[inline]
    pub fn is_truncated(&self) -> bool {
        self.omitted_count() != 0
    }

    /// Returns whether no subscription close failed.
    ///
    /// # Returns
    /// `true` when the snapshot contains no close failures.
    #[must_use]
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.total_count == 0
    }

    /// Iterates over retained failed-subscription samples and their provider
    /// errors.
    ///
    /// # Returns
    /// A borrowing iterator over at most 256 retained samples in recorded
    /// order. This may omit failures; use [`Self::total_count`] for the
    /// complete count.
    #[must_use = "Use the returned iter."]
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &SubscriptionCloseFailure> {
        self.failures.iter().map(Arc::as_ref)
    }
}

impl fmt::Display for SubscriptionCloseErrors {
    /// Formats the total failure count and retained failure samples.
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
        write!(
            formatter,
            "{} subscription close failure(s) ({} retained, {} omitted)",
            self.total_count,
            self.len(),
            self.omitted_count(),
        )?;
        for failure in &self.failures {
            write!(formatter, "; {failure}")?;
        }
        Ok(())
    }
}

impl Error for SubscriptionCloseErrors {
    /// Exposes the first retained close failure; [`Self::iter`] also yields
    /// retained samples only, while [`Self::total_count`] reports the full
    /// count.
    ///
    /// # Returns
    /// The first retained close failure, or `None` when no sample was retained.
    #[inline]
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(Arc::as_ref)
            .map(|failure| failure as &(dyn Error + 'static))
    }
}
