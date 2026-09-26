// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Errors returned when notification admission fails.

use std::fmt;

/// Returns the original payload when a notification cannot enter the queue.
pub enum TryPublishError<T> {
    /// The bounded local queue is full.
    Full(T),
    /// The publisher is closing or already closed.
    Closed(T),
}

impl<T> fmt::Debug for TryPublishError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("Full(..)"),
            Self::Closed(_) => formatter.write_str("Closed(..)"),
        }
    }
}

impl<T> fmt::Display for TryPublishError<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full(_) => formatter.write_str("notification queue is full"),
            Self::Closed(_) => formatter.write_str("notification publisher is closed"),
        }
    }
}

impl<T: Send + Sync + 'static> std::error::Error for TryPublishError<T> {}
