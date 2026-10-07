// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Cancellation accounting for an already-entered settlement SPI operation.

use std::io::Error;
use std::io::ErrorKind;
use std::sync::Arc;

use qubit_clock::Timer;

use super::settlement_progress::SettlementProgress;
use crate::error::SpiError;
use crate::facade::internal::SettlementRetryDecision;

/// Records an interrupted SPI attempt only if its future is actually cancelled.
///
/// # Type Parameters
/// - `'a`: Exclusive borrow of accounting retained in the caller-owned session.
#[must_use = "dropping this guard records an interrupted settlement attempt"]
pub(in crate::facade) struct SettlementAttemptGuard<'a> {
    /// Persistent state, disjoint from the receiver and token borrows.
    pub(in crate::facade) progress: &'a mut SettlementProgress,
    /// Clock and cancellation error identity.
    pub(in crate::facade) timer: Arc<dyn Timer>,
    /// Provider identity used by the original operation.
    pub(in crate::facade) provider_id: Box<str>,
    /// False after any provider response, including failures.
    pub(in crate::facade) armed: bool,
}
impl Drop for SettlementAttemptGuard<'_> {
    /// Retains interrupted-attempt context and backoff without publishing a
    /// stop.
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let error = Arc::new(SpiError::Operation {
            provider_id: self.provider_id.clone(),
            operation: "settle",
            resource: None,
            kind: "settlement_attempt_cancelled",
            retryable: Some(true),
            source: Box::new(Error::new(
                ErrorKind::Interrupted,
                "caller paused an in-flight settlement attempt",
            )),
        });
        match self.progress.elapsed(self.timer.as_ref()) {
            Ok(elapsed) => {
                if let SettlementRetryDecision::RetryAfter(delay) = self.progress.retry.after_error(&error, elapsed) {
                    self.progress.due = elapsed.saturating_add(delay);
                }
            }
            Err(error) => self.progress.infrastructure_error = Some(error),
        }
        self.progress.last_error = Some(error);
        self.progress.cancelled = true;
    }
}
