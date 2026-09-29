// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscriber error-handler decisions for a failed delivery.

/// The next action requested by a subscriber delivery error handler.
///
/// This directive applies to delivery lifecycle decisions. Publish error
/// handlers are terminal observers and return `()`.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::FailureDirective;
///
/// let directive = FailureDirective::DeadLetter;
/// assert_eq!(directive, FailureDirective::DeadLetter);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum FailureDirective {
    /// Let the configured delivery retry policy make another attempt.
    Retry,
    /// Ask a capable provider to deliver the message again.
    Requeue,
    /// Send the failed event through the dead-letter policy.
    DeadLetter,
    /// Stop processing this delivery failure.
    Discard,
}
