// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Persistent per-delivery attempt budget and cancellation accounting.

use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;

use crate::error::SpiError;
use crate::facade::SettlementRetryConfig;
use crate::facade::internal::SettlementRetryState;

/// Settlement state outlives every caller-owned run future.
pub(in crate::facade) struct SettlementProgress {
    /// Pure finite retry policy.
    pub(in crate::facade) retry: SettlementRetryState,
    /// Clock origin captured immediately before the first SPI attempt.
    pub(in crate::facade) started_at: Option<MonotonicInstant>,
    /// Next attempt deadline measured from the origin.
    pub(in crate::facade) due: Duration,
    /// Original provider or cancellation source retained across attempts.
    pub(in crate::facade) last_error: Option<Arc<SpiError>>,
    /// Timer registration retained across pauses.
    pub(in crate::facade) timer: Option<TimerFuture>,
    /// Timer failure deferred to the receiver owner.
    pub(in crate::facade) infrastructure_error: Option<TimeError>,
    /// A successful SPI response remains true even if duration accounting
    /// fails.
    pub(in crate::facade) confirmed: bool,
    /// Cancellation is classified on resume, never published during pause.
    pub(in crate::facade) cancelled: bool,
}
impl SettlementProgress {
    /// Starts with no attempt, timer registration, or confirmed settlement.
    ///
    /// # Parameters
    /// - `config`: Validated finite attempt and elapsed-time limits.
    ///
    /// # Returns
    /// Persistent accounting ready for its first actual SPI attempt.
    #[must_use]
    #[inline]
    pub(in crate::facade) fn new(config: SettlementRetryConfig) -> Self {
        Self {
            retry: SettlementRetryState::new(config),
            started_at: None,
            due: Duration::ZERO,
            last_error: None,
            timer: None,
            infrastructure_error: None,
            confirmed: false,
            cancelled: false,
        }
    }
    /// Reads elapsed time in the timer's own monotonic domain.
    ///
    /// # Parameters
    /// - `timer`: Timer supplying the same clock domain used at the first
    ///   attempt.
    ///
    /// # Returns
    /// Time since the first SPI attempt, or zero before an attempt has started.
    ///
    /// # Errors
    /// Returns `TimeError::ClockDomainMismatch` if the clock domain changed, or
    /// `TimeError::InvalidInstantOrder` if the sampled instant precedes the
    /// origin.
    #[inline]
    pub(in crate::facade) fn elapsed(&self, timer: &dyn Timer) -> Result<Duration, TimeError> {
        self.started_at.map_or(Ok(Duration::ZERO), |start| {
            timer.clock().now().duration_since(start)
        })
    }
}
