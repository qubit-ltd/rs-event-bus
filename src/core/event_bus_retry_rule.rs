// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Default retry classification for local event-bus operations.

use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryRule;

use crate::EventBusError;

/// Retries handler, interceptor and executor-rejection failures only.
///
/// Configuration, type, lifecycle, panic and retry-infrastructure failures are
/// terminal. Applications can register a rule before this default through
/// publish or subscribe options; `UseDefault` falls through to this rule.
#[derive(Debug, Clone, Copy, Default)]
pub struct EventBusRetryRule;

impl RetryRule<EventBusError> for EventBusRetryRule {
    /// Classifies recoverable application failures without changing admission
    /// limits.
    fn decide(&self, failure: &AttemptFailure<EventBusError>, _context: &RetryContext) -> RetryDecision {
        match failure {
            AttemptFailure::Error(
                EventBusError::HandlerFailed { .. }
                | EventBusError::InterceptorFailed { .. }
                | EventBusError::ExecutionRejected { .. },
            ) => RetryDecision::Retry,
            _ => RetryDecision::Abort,
        }
    }
}
