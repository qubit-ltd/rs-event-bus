// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus diagnostics operations.

use std::sync::Arc;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;

use crate::AsyncEventBus;
use crate::Diagnostic;
use crate::DiagnosticObserverHandle;
use crate::error::SpiError;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::observer_entry::ObserverEntry;
use crate::pipeline::DiagnosticObserver;

impl AsyncEventBus {
    /// Returns bounded active gauges and cumulative delivery lifecycle
    /// counters. Clock failures are diagnosed and stop affected work;
    /// gauges remain accurate.
    ///
    /// # Returns
    /// Current bounded gauges and bus totals, omitting age if the clock failed.
    #[must_use = "delivery metrics are the current bus diagnostics"]
    pub fn delivery_metrics(&self) -> DeliveryMetricsSnapshot {
        let input = self.inner.scheduler.snapshot_input(None);
        let now = self.inner.timer.clock().now();
        let gauges = match input.at(now) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let message = error.to_string();
                let error = Arc::new(SpiError::Operation {
                    provider_id: self.inner.provider_id.as_str().into(),
                    operation: "delivery_metrics",
                    resource: None,
                    kind: "delivery_metrics_clock_failure",
                    retryable: Some(false),
                    source: Box::new(error),
                });
                let controls: Vec<_> = self
                    .inner
                    .controls
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .values()
                    .cloned()
                    .collect();
                let mut first = false;
                for control in controls {
                    first |= control.fail_metrics_clock(error.clone());
                }
                if first {
                    self.inner.emit(&Diagnostic::InternalFailure {
                        origin: "delivery_metrics_clock".into(),
                        message: message.into(),
                    });
                }
                self.inner.scheduler.snapshot_gauges(None)
            }
        };
        self.inner.delivery_metrics.snapshot(gauges)
    }

    /// Registers a synchronous diagnostic observer until the returned handle is
    /// dropped.
    ///
    /// # Type Parameters
    /// - `F`: Thread-safe callback type.
    ///
    /// # Parameters
    /// - `observer`: Callback invoked for emitted diagnostics.
    ///
    /// # Returns
    /// A handle that unregisters the observer when dropped.
    #[must_use = "the handle keeps the diagnostic observer registered"]
    pub fn observe_diagnostics<F>(&self, observer: F) -> DiagnosticObserverHandle
    where
        F: Fn(&Diagnostic) + Send + Sync + 'static,
    {
        let entry = Arc::new(ObserverEntry {
            active: AtomicBool::new(true),
            callback: Arc::new(observer),
        });
        let mut entries = self
            .inner
            .observers
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        entries.retain(|entry| entry.strong_count() > 0);
        entries.push(Arc::downgrade(&entry));
        DiagnosticObserverHandle::new(entry)
    }

    /// Returns the live diagnostic callbacks registered with this bus.
    ///
    /// # Returns
    /// Strong references to observers that are still registered.
    #[must_use]
    #[inline]
    pub(super) fn observer_snapshot(&self) -> Vec<Arc<DiagnosticObserver>> {
        self.inner.observer_snapshot()
    }
}
