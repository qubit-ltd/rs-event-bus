// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned when notification admission fails.

use std::error::Error;
use std::fmt;

/// Returns the original payload when a notification cannot enter the queue.
///
/// # Type Parameters
/// - `T`: Payload type returned to the caller when queue admission fails.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::TryPublishError;
///
/// let error = TryPublishError::Full("payload");
/// assert!(matches!(error, TryPublishError::Full("payload")));
/// ```
#[must_use]
pub enum TryPublishError<T> {
    /// The bounded local queue is full.
    Full(
        /// Original payload returned to the caller for retry or recovery.
        T,
    ),
    /// The publisher is closing or already closed.
    Closed(
        /// Original payload returned to the caller for retry or recovery.
        T,
    ),
}

impl<T> fmt::Debug for TryPublishError<T> {
    /// Formats the variant without exposing or requiring `Debug` for the
    /// payload.
    ///
    /// # Parameters
    /// - `formatter`: destination for the variant name.
    ///
    /// # Returns
    /// The formatter result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("Full(..)"),
            Self::Closed(_) => formatter.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TryPublishError<T> {
    /// Formats the queue-admission failure without exposing the payload.
    ///
    /// # Parameters
    /// - `formatter`: destination for the admission error text.
    ///
    /// # Returns
    /// The formatter result.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("notification queue is full"),
            Self::Closed(_) => formatter.write_str("notification publisher is closed"),
        }
    }
}

impl<T: Send + Sync + 'static> Error for TryPublishError<T> {}
