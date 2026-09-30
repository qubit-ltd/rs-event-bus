// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous subscription state.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::task::Poll;

use qubit_id::Id;

use super::super::AsyncEventBusInner;
use super::super::AsyncShutdownDriver;
use super::super::SignalRegistration;
use super::AsyncSession;
use super::SessionLease;
use super::SessionSignals;
use super::SessionSlot;
use crate::error::ReceiveError;
use crate::error::SpiError;
use crate::error::SubscriptionCloseFailure;
use crate::facade::DeliveryMetricsSnapshot;
use crate::facade::async_event_bus::AsyncSignal;
use crate::facade::internal::DeliveryMetrics;
use crate::model::SubscriptionStopReason;
use crate::pipeline::Diagnostic;
use crate::spi::ShutdownMode;

/// Coordinates exclusive access to one paused or running session.
///
/// # Type Parameters
/// - `T`: payload type handled by this subscription.
pub(in crate::facade) struct AsyncSubscriptionControl<T: 'static> {
    /// Shared stop state observed by the session.
    pub(in crate::facade::async_subscription) signals: Arc<SessionSignals>,
    /// Session lease and disposal state.
    pub(in crate::facade::async_subscription) slot: Mutex<SessionSlot<T>>,
    /// Wakes callers waiting for the session lease.
    pub(in crate::facade::async_subscription) available: AsyncSignal,
    /// Canonical provider receiver close failure.
    pub(in crate::facade::async_subscription) close_error: Mutex<Option<Arc<SubscriptionCloseFailure>>>,
    /// Weak owner used to unregister this control from the bus.
    pub(in crate::facade::async_subscription) bus: Weak<AsyncEventBusInner>,
    /// Final cumulative counters retained after receiver cleanup.
    pub(in crate::facade) metrics: Arc<DeliveryMetrics>,
    /// Bus-local subscription identity.
    pub(in crate::facade::async_subscription) id: Id,
}

impl<T: Send + Sync + 'static> AsyncSubscriptionControl<T> {
    /// Creates the shared lease coordinator for a new session.
    ///
    /// # Parameters
    /// - `session`: receiver and runner state transferred into this control.
    /// - `signals`: stop signal shared with the session.
    ///
    /// # Returns
    /// Shared control registered with the originating bus.
    #[must_use = "the subscription control must be retained"]
    pub(in crate::facade::async_subscription) fn new(
        session: AsyncSession<T>,
        signals: Arc<SessionSignals>,
    ) -> Arc<Self> {
        let bus = Arc::downgrade(&session.inner);
        let id = session.id;
        let metrics = session.metrics.clone();
        Arc::new(Self {
            signals,
            metrics,
            slot: Mutex::new(SessionSlot {
                session: Some(session),
                active: false,
                disposed: false,
            }),
            available: AsyncSignal::default(),
            close_error: Mutex::new(None),
            bus,
            id,
        })
    }

    /// Waits until this caller can exclusively own the session.
    ///
    /// # Returns
    /// `Some` with a lease while the control is live, or `None` after disposal.
    #[must_use = "the session lease result must be handled"]
    pub(in crate::facade::async_subscription) async fn lease(&self) -> Option<SessionLease<'_, T>> {
        let registration = SignalRegistration::new(&self.available);
        let session = poll_fn(|cx| {
            let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.disposed {
                return Poll::Ready(None);
            }
            if !slot.active
                && let Some(session) = slot.session.take()
            {
                slot.active = true;
                return Poll::Ready(Some(session));
            }
            registration.register(cx.waker());
            Poll::Pending
        })
        .await;
        session.map(|session| SessionLease {
            control: self,
            session: Some(session),
        })
    }
}

impl<T: 'static> AsyncSubscriptionControl<T> {
    /// Merges live scheduler gauges with retained final counters after close.
    ///
    /// # Returns
    /// Subscription counters with active gauges, or zero gauges after disposal.
    /// Clock errors publish the first stop cause and return accurate gauges
    /// without age.
    #[must_use = "delivery metrics are the current subscription diagnostics"]
    #[inline]
    pub(in crate::facade::async_subscription) fn delivery_metrics(&self) -> DeliveryMetricsSnapshot {
        let gauges = self.bus.upgrade().map_or_else(Default::default, |bus| {
            let input = bus.scheduler.snapshot_input(Some(self.id));
            let now = bus.timer.clock().now();
            match input.at(now) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    let message = error.to_string();
                    let error = Arc::new(SpiError::Operation {
                        provider_id: bus.provider_id.as_str().into(),
                        operation: "delivery_metrics",
                        resource: None,
                        kind: "delivery_metrics_clock_failure",
                        retryable: Some(false),
                        source: Box::new(error),
                    });
                    if self.signals.fail_receive(SubscriptionStopReason::Provider { error }) {
                        bus.emit(&Diagnostic::InternalFailure {
                            origin: "delivery_metrics_clock".into(),
                            message: message.into(),
                        });
                    }
                    bus.scheduler.snapshot_gauges(Some(self.id))
                }
            }
        });
        self.metrics.snapshot(gauges)
    }

    /// Stops the subscription and relinquishes its receiver on handle drop.
    ///
    /// This synchronous disposal cannot await provider close; bus shutdown or
    /// explicit [`AsyncSubscription::close`] performs
    /// asynchronous cleanup.
    pub(in crate::facade::async_subscription) fn dispose(&self) {
        self.signals.stop(ShutdownMode::Immediate);
        let session = {
            let mut slot = self.slot.lock().unwrap_or_else(PoisonError::into_inner);
            slot.disposed = true;
            if slot.active { None } else { slot.session.take() }
        };
        if let Some(bus) = self.bus.upgrade() {
            bus.controls
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&self.id);
        }
        drop(session);
        self.available.notify();
    }
}

impl<T: Send + Sync + 'static> AsyncShutdownDriver for AsyncSubscriptionControl<T> {
    /// Wakes the executor that currently drives this session.
    #[inline]
    fn notify(&self) {
        self.signals.signal().notify();
    }
    /// Stores a metrics clock error before any diagnostic callback can reenter.
    ///
    /// # Parameters
    /// - `error`: Original clock failure retained as the provider stop source.
    ///
    /// # Returns
    /// True only when this call publishes the first terminal cause.
    fn fail_metrics_clock(&self, error: Arc<SpiError>) -> bool {
        self.signals.fail_receive(SubscriptionStopReason::Provider { error })
    }
    /// Stops the active or future runner.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy applied to this subscription.
    #[inline]
    fn stop(&self, mode: ShutdownMode) {
        self.signals.stop(mode);
    }

    /// Returns the canonical receiver close failure, when one exists.
    ///
    /// # Returns
    /// The stored close failure, or `None` before a close failure occurs.
    #[inline]
    fn close_error(&self) -> Option<Arc<SubscriptionCloseFailure>> {
        self.close_error.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Stores the first close failure for later shutdown callers.
    ///
    /// # Parameters
    /// - `failure`: receiver close failure observed by a caller.
    ///
    /// # Returns
    /// The first stored failure.
    fn store_close_error(&self, failure: Arc<SubscriptionCloseFailure>) -> Arc<SubscriptionCloseFailure> {
        let mut stored = self.close_error.lock().unwrap_or_else(PoisonError::into_inner);
        stored.get_or_insert(failure).clone()
    }

    /// Stops the session, resumes its runner if needed, and closes its
    /// receiver.
    ///
    /// # Parameters
    /// - `mode`: shutdown mode requested by the bus.
    ///
    /// # Returns
    /// A future that completes after session and receiver cleanup.
    ///
    /// # Errors
    /// The future resolves with the canonical provider receiver close failure.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<SubscriptionCloseFailure>>> + Send + 'a>> {
        Box::pin(async move {
            self.signals.stop(mode);
            let Some(mut lease) = self.lease().await else {
                return Ok(());
            };
            let Some(session) = lease.session.as_mut() else {
                return Ok(());
            };
            if let Some(handler) = session.handler.clone()
                && let Err(error) = session.run_loop(handler).await
                && !matches!(error, ReceiveError::Stopped(_))
            {
                session.inner.emit(&Diagnostic::InternalFailure {
                    origin: "shutdown_resume".into(),
                    message: error.to_string().into(),
                });
            }
            session.close_inner(self).await
        })
    }
}
