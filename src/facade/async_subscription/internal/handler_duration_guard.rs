// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Actual handler invocation timing, retained inside the owned handler future.

use std::sync::Arc;

use qubit_clock::MonotonicInstant;

use super::SessionSignals;
use crate::facade::async_event_bus::AsyncEventBusInner;
use crate::facade::internal::DeliveryMetrics;

/// Captures normal return and unwinding without counting filter-only work.
pub(super) struct HandlerDurationGuard {
    /// Clock and observer context.
    inner: Arc<AsyncEventBusInner>,
    /// Subscription-local accumulator forwarding totals to the bus.
    metrics: Arc<DeliveryMetrics>,
    /// Owner stop channel for clock failure.
    signals: Arc<SessionSignals>,
    /// Start in the injected timer's monotonic domain.
    started: MonotonicInstant,
}
impl HandlerDurationGuard {
    /// Begins immediately before invoking the application handler factory.
    ///
    /// # Parameters
    /// - `inner`: Bus supplying the timer and diagnostic observers.
    /// - `metrics`: Subscription counters receiving the duration sample.
    /// - `signals`: First-cause gate used when duration validation fails.
    ///
    /// # Returns
    /// A guard that survives asynchronous suspension until this invocation
    /// ends. Creation samples the timer clock; drop may stop the owner and
    /// notify observers.
    #[must_use = "the guard must remain alive until the handler invocation ends"]
    pub(super) fn new(
        inner: Arc<AsyncEventBusInner>,
        metrics: Arc<DeliveryMetrics>,
        signals: Arc<SessionSignals>,
    ) -> Self {
        let started = inner.timer.clock().now();
        Self {
            inner,
            metrics,
            signals,
            started,
        }
    }
}
impl Drop for HandlerDurationGuard {
    /// Records duration, or gates the original clock error before notifying
    /// observers.
    fn drop(&mut self) {
        if let Err(error) = self
            .metrics
            .record_handler_duration(self.started, self.inner.timer.clock().now())
        {
            let error = Arc::new(crate::error::SpiError::Operation {
                provider_id: self.inner.provider_id.as_str().into(),
                operation: "handler",
                resource: None,
                kind: "handler_clock_failure",
                retryable: Some(false),
                source: Box::new(error),
            });
            let message = error.to_string();
            if self
                .signals
                .fail_receive(crate::model::SubscriptionStopReason::Provider { error })
            {
                self.inner.emit(&crate::Diagnostic::InternalFailure {
                    origin: "handler_clock".into(),
                    message: message.into(),
                });
            }
        }
    }
}
