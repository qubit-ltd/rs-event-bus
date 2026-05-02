/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Retry options for publisher and subscriber processing.

use std::time::Duration;

use crate::{EventBusError, EventBusResult};

/// Simple retry settings shared by publish and subscribe paths.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct RetryOptions {
    max_attempts: usize,
    delay: Duration,
}

impl RetryOptions {
    /// Creates retry options.
    ///
    /// # Parameters
    /// - `max_attempts`: Maximum attempts including the first attempt.
    /// - `delay`: Delay between failed attempts.
    ///
    /// # Returns
    /// Retry options accepted by event bus APIs.
    ///
    /// # Errors
    /// Returns [`EventBusError::InvalidArgument`] when `max_attempts` is zero.
    pub fn new(max_attempts: usize, delay: Duration) -> EventBusResult<Self> {
        if max_attempts == 0 {
            return Err(EventBusError::invalid_argument(
                "max_attempts",
                "retry max_attempts must be greater than zero",
            ));
        }
        Ok(Self {
            max_attempts,
            delay,
        })
    }

    /// Returns the maximum attempt count.
    ///
    /// # Returns
    /// Attempts including the first handler invocation.
    pub const fn max_attempts(&self) -> usize {
        self.max_attempts
    }

    /// Returns the delay between failed attempts.
    ///
    /// # Returns
    /// Retry delay duration.
    pub const fn delay(&self) -> Duration {
        self.delay
    }
}

impl Default for RetryOptions {
    /// Creates options that run only one attempt.
    fn default() -> Self {
        Self {
            max_attempts: 1,
            delay: Duration::ZERO,
        }
    }
}
