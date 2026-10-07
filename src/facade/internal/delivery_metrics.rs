// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded cumulative delivery statistics with one-way subscription ownership.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;

use crate::facade::DeliveryMetricsSnapshot;

/// Stores fixed-size counters and an optional bus accumulator, never delivery
/// history.
#[derive(Default)]
pub(in crate::facade) struct DeliveryMetrics {
    /// Cumulative fields only; scheduler metadata supplies every active gauge.
    counters: Mutex<DeliveryMetricsSnapshot>,
    /// Shared bus accumulator retained by a subscription, without a reverse
    /// reference.
    parent: Option<Arc<Self>>,
}

impl DeliveryMetrics {
    /// Creates subscription counters forwarding each update to `parent` after
    /// unlocking.
    ///
    /// # Parameters
    /// - `parent`: Bus accumulator, which must not itself have a parent.
    ///
    /// # Returns
    /// Empty subscription counters retaining only the global accumulator.
    #[must_use]
    #[inline]
    pub(in crate::facade) fn new_subscription(parent: Arc<Self>) -> Self {
        Self {
            counters: Mutex::default(),
            parent: Some(parent),
        }
    }

    /// Returns local counters combined with scheduler-owned `gauges`, without
    /// callbacks.
    ///
    /// # Parameters
    /// - `gauges`: Active phase counts and checked oldest age from the
    ///   scheduler.
    ///
    /// # Returns
    /// A fixed-size value that retains no delivery or subscription state.
    pub(in crate::facade) fn snapshot(&self, gauges: DeliveryMetricsSnapshot) -> DeliveryMetricsSnapshot {
        let mut counters = *self.counters.lock().unwrap_or_else(PoisonError::into_inner);
        counters.reserved_receives = gauges.reserved_receives;
        counters.queued = gauges.queued;
        counters.running_handlers = gauges.running_handlers;
        counters.settling = gauges.settling;
        counters.lane_waiting = gauges.lane_waiting;
        counters.oldest_owned_age = gauges.oldest_owned_age;
        counters
    }

    /// Records actual SPI entry for `attempt`; rejected admissions must not
    /// call this.
    ///
    /// # Parameters
    /// - `attempt`: One-based attempt number for this delivery's settlement
    ///   cycle.
    pub(in crate::facade) fn record_settlement_attempt(&self, attempt: u32) {
        self.update(|counters| {
            counters.settlement_attempts = counters.settlement_attempts.saturating_add(1);
            if attempt > 1 {
                counters.settlement_retries = counters.settlement_retries.saturating_add(1);
            }
        });
    }

    /// Records one permanently stopped settlement cycle.
    pub(in crate::facade) fn record_terminal_failure(&self) {
        self.update(|counters| {
            counters.settlement_terminal_failures = counters.settlement_terminal_failures.saturating_add(1)
        });
    }

    /// Records one successfully completed delivery, excluding unresolved
    /// cleanup.
    pub(in crate::facade) fn record_completed(&self) {
        self.update(|counters| counters.completed = counters.completed.saturating_add(1));
    }

    /// Records one unresolved known ephemeral delivery abandoned during
    /// cleanup.
    pub(in crate::facade) fn record_abandoned_ephemeral(&self) {
        self.update(|counters| counters.abandoned_ephemeral = counters.abandoned_ephemeral.saturating_add(1));
    }

    /// Checks a handler interval before recording a sample in both
    /// accumulators.
    ///
    /// # Parameters
    /// - `started`: Monotonic instant immediately before invoking the user
    ///   handler.
    /// - `ended`: Same-domain instant when that invocation finishes or panics.
    ///
    /// # Returns
    /// Success after recording one handler invocation, including a failed
    /// attempt.
    ///
    /// # Errors
    /// Returns the clock-domain or backwards-time error without updating any
    /// counters.
    pub(in crate::facade) fn record_handler_duration(
        &self,
        started: MonotonicInstant,
        ended: MonotonicInstant,
    ) -> Result<(), TimeError> {
        self.record_handler_elapsed(ended.duration_since(started)?);
        Ok(())
    }

    /// Checks a complete settlement interval, including retries, before
    /// recording it.
    ///
    /// # Parameters
    /// - `started`: Monotonic origin before the first actual SPI settlement
    ///   attempt.
    /// - `ended`: Same-domain instant at successful or terminal completion.
    ///
    /// # Returns
    /// Success after recording one complete settlement cycle.
    ///
    /// # Errors
    /// Returns the clock-domain or backwards-time error without updating any
    /// counters.
    pub(in crate::facade) fn record_settlement_duration(
        &self,
        started: MonotonicInstant,
        ended: MonotonicInstant,
    ) -> Result<(), TimeError> {
        self.record_settlement_elapsed(ended.duration_since(started)?);
        Ok(())
    }

    /// Records one already-checked handler `elapsed` duration, saturating
    /// nanoseconds.
    ///
    /// # Parameters
    /// - `elapsed`: Duration of one actual user handler invocation, including
    ///   failures.
    pub(in crate::facade) fn record_handler_elapsed(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.update(|counters| {
            counters.handler_duration_count = counters.handler_duration_count.saturating_add(1);
            counters.handler_duration_total_nanos = counters.handler_duration_total_nanos.saturating_add(nanos);
            counters.handler_duration_max_nanos = counters.handler_duration_max_nanos.max(nanos);
        });
    }

    /// Records one already-checked settlement `elapsed` duration, including
    /// retry waits.
    ///
    /// # Parameters
    /// - `elapsed`: Duration from the first SPI attempt to successful or
    ///   terminal completion.
    pub(in crate::facade) fn record_settlement_elapsed(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.update(|counters| {
            counters.settlement_duration_count = counters.settlement_duration_count.saturating_add(1);
            counters.settlement_duration_total_nanos = counters.settlement_duration_total_nanos.saturating_add(nanos);
            counters.settlement_duration_max_nanos = counters.settlement_duration_max_nanos.max(nanos);
        });
    }

    /// Applies a private counter operation, releasing each lock before updating
    /// its parent.
    ///
    /// # Type Parameters
    /// - `F`: Internal arithmetic operation without callbacks or external side
    ///   effects.
    ///
    /// # Parameters
    /// - `record`: Operation applied once to this accumulator and once to the
    ///   bus.
    fn update<F: Fn(&mut DeliveryMetricsSnapshot)>(&self, record: F) {
        {
            let mut counters = self.counters.lock().unwrap_or_else(PoisonError::into_inner);
            record(&mut counters);
        }
        if let Some(parent) = &self.parent {
            let mut counters = parent.counters.lock().unwrap_or_else(PoisonError::into_inner);
            record(&mut counters);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use qubit_clock::ManualMonotonicClock;
    use qubit_clock::MonotonicClock;

    use super::DeliveryMetrics;
    use crate::facade::DeliveryMetricsSnapshot;

    #[test]
    fn test_private_clock_errors_leave_both_accumulators_untouched() {
        let global = Arc::new(DeliveryMetrics::default());
        let local = DeliveryMetrics::new_subscription(global.clone());
        let clock = ManualMonotonicClock::new();
        let foreign = ManualMonotonicClock::new();
        let first = clock.now();
        clock.advance(Duration::from_nanos(5)).unwrap();
        let later = clock.now();
        assert!(local.record_handler_duration(later, first).is_err());
        assert!(local.record_settlement_duration(first, foreign.now()).is_err());
        assert_eq!(*local.counters.lock().unwrap(), DeliveryMetricsSnapshot::default());
        assert_eq!(*global.counters.lock().unwrap(), DeliveryMetricsSnapshot::default());
        local.record_handler_duration(first, later).unwrap();
        local.record_settlement_duration(first, later).unwrap();
        assert_eq!(local.counters.lock().unwrap().handler_duration_total_nanos, 5);
        assert_eq!(global.counters.lock().unwrap().settlement_duration_total_nanos, 5);
    }

    #[test]
    fn test_private_delivery_counters_saturate_without_retaining_subscription_history() {
        let global = Arc::new(DeliveryMetrics::default());
        let local = DeliveryMetrics::new_subscription(global.clone());
        local.counters.lock().unwrap().settlement_attempts = u64::MAX - 1;
        local.counters.lock().unwrap().handler_duration_total_nanos = u64::MAX - 1;
        local.record_settlement_attempt(1);
        local.record_settlement_attempt(2);
        local.record_handler_elapsed(Duration::MAX);
        local.record_settlement_elapsed(Duration::MAX);
        local.record_terminal_failure();
        local.record_completed();
        local.record_abandoned_ephemeral();
        let own = local.snapshot(DeliveryMetricsSnapshot::default());
        assert_eq!(own.settlement_attempts, u64::MAX);
        assert_eq!(own.settlement_retries, 1);
        assert_eq!(own.handler_duration_count, 1);
        assert_eq!(own.handler_duration_total_nanos, u64::MAX);
        assert_eq!(own.handler_duration_max_nanos, u64::MAX);
        assert_eq!(own.settlement_duration_count, 1);
        assert_eq!(own.settlement_duration_total_nanos, u64::MAX);
        assert_eq!(own.settlement_duration_max_nanos, u64::MAX);
        assert_eq!(own.completed, 1);
        assert_eq!(own.abandoned_ephemeral, 1);
        assert_eq!(own.settlement_terminal_failures, 1);
        drop(local);
        assert_eq!(Arc::strong_count(&global), 1);
        let bus = global.snapshot(DeliveryMetricsSnapshot::default());
        assert_eq!(bus.settlement_attempts, 2);
        assert_eq!(bus.settlement_retries, 1);
        assert_eq!(bus.completed, 1);
    }
}
