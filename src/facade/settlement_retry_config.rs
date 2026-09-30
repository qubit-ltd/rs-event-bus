// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Validated facade finite settlement retry policy.

use std::num::NonZeroU32;
use std::time::Duration;

use crate::error::ConfigurationError;

/// Finite settlement retries; only explicitly retryable provider errors may
/// retry.
///
/// The elapsed budget starts immediately before the first SPI attempt and
/// accumulates across attempts and backoff. It is checked between attempts,
/// before starting another retry; it does not forcibly cancel an in-flight
/// synchronous or asynchronous SPI call. A successful result returned after
/// the deadline is still accepted. An indefinitely blocked SPI call therefore
/// can outlive this budget and delay shutdown.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
///
/// use qubit_event_bus::facade::SettlementRetryConfig;
///
/// let config = SettlementRetryConfig::default();
/// assert_eq!(config.max_attempts().get(), 5);
/// assert_eq!(config.max_elapsed(), Duration::from_secs(5));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SettlementRetryConfig {
    /// Maximum settlement attempts, including the first attempt.
    max_attempts: NonZeroU32,
    /// Maximum elapsed budget accumulated from immediately before the first SPI
    /// attempt.
    max_elapsed: Duration,
    /// Delay before the first settlement retry.
    initial_backoff: Duration,
    /// Maximum delay between settlement attempts.
    max_backoff: Duration,
}

impl SettlementRetryConfig {
    /// Creates a validated settlement retry policy.
    ///
    /// # Parameters
    /// - `max_attempts`: Maximum settlement attempts, including the first
    ///   attempt.
    /// - `max_elapsed`: Maximum elapsed budget accumulated from immediately
    ///   before the first SPI attempt, checked between attempts. In-flight SPI
    ///   calls are not forcibly cancelled; late success is still accepted.
    /// - `initial_backoff`: Delay before the first settlement retry.
    /// - `max_backoff`: Maximum delay between settlement attempts.
    ///
    /// # Returns
    /// The validated policy.
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` when `max_elapsed` is zero.
    /// Returns `ConfigurationError::InvalidField` when `initial_backoff` is
    /// zero. Returns `ConfigurationError::InvalidField` when `max_backoff`
    /// is less than `initial_backoff`.
    pub fn new(
        max_attempts: NonZeroU32,
        max_elapsed: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<Self, ConfigurationError> {
        if max_elapsed.is_zero() {
            return Err(ConfigurationError::InvalidField {
                field: "max_elapsed",
                message: "must be positive".into(),
            });
        }
        if initial_backoff.is_zero() {
            return Err(ConfigurationError::InvalidField {
                field: "initial_backoff",
                message: "must be positive".into(),
            });
        }
        if max_backoff < initial_backoff {
            return Err(ConfigurationError::InvalidField {
                field: "max_backoff",
                message: "must not be less than initial_backoff".into(),
            });
        }
        Ok(Self {
            max_attempts,
            max_elapsed,
            initial_backoff,
            max_backoff,
        })
    }

    /// Returns maximum settlement attempts, including the first attempt.
    ///
    /// # Returns
    /// Maximum settlement attempts, including the first attempt.
    #[must_use]
    #[inline]
    pub const fn max_attempts(&self) -> NonZeroU32 {
        self.max_attempts
    }

    /// Returns the elapsed budget accumulated from immediately before the first
    /// SPI attempt.
    ///
    /// This budget is checked between attempts before retrying. It does not
    /// forcibly cancel an in-flight SPI call; success arriving after the
    /// deadline is still accepted.
    ///
    /// # Returns
    /// Maximum elapsed budget accumulated from immediately before the first SPI
    /// attempt.
    #[must_use]
    #[inline]
    pub const fn max_elapsed(&self) -> Duration {
        self.max_elapsed
    }

    /// Returns delay before the first settlement retry.
    ///
    /// # Returns
    /// Delay before the first settlement retry.
    #[must_use]
    #[inline]
    pub const fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Returns maximum delay between settlement attempts.
    ///
    /// # Returns
    /// Maximum delay between settlement attempts.
    #[must_use]
    #[inline]
    pub const fn max_backoff(&self) -> Duration {
        self.max_backoff
    }
}

impl Default for SettlementRetryConfig {
    /// Returns the standard finite facade policy.
    ///
    /// # Returns
    /// A policy with five attempts, a five-second elapsed budget,
    /// ten-millisecond initial backoff, and one-second maximum backoff.
    fn default() -> Self {
        Self {
            max_attempts: NonZeroU32::new(5).expect("positive default limit"),
            max_elapsed: Duration::from_secs(5),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
        }
    }
}
