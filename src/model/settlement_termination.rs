// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Terminal settlement classifications.

/// Why settlement stopped without a successful terminal disposition.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::SettlementTermination;
///
/// let termination = SettlementTermination::AttemptsExhausted;
/// assert_eq!(termination, SettlementTermination::AttemptsExhausted);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementTermination {
    /// Provider explicitly classified the error as permanent.
    PermanentError,
    /// Provider did not classify retryability.
    RetryabilityUnknown,
    /// The configured total attempt limit was reached.
    AttemptsExhausted,
    /// The monotonic settlement deadline was reached.
    DeadlineExceeded,
    /// Provider settlement code panicked.
    ProviderPanicked,
    /// The settlement token was rejected or belonged to another owner.
    InvalidToken,
    /// Timer, clock, or owner infrastructure failed.
    InfrastructureFailure,
}
