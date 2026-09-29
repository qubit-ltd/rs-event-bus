// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::Diagnostic;
use crate::facade::async_subscription::Arc;
use crate::facade::async_subscription::AsyncEventBusInner;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::AsyncShutdownDriver;
use crate::facade::async_subscription::AsyncSignal;
use crate::facade::async_subscription::Id;
use crate::facade::async_subscription::Mutex;
use crate::facade::async_subscription::Pin;
use crate::facade::async_subscription::SessionSignals;
use crate::facade::async_subscription::SignalRegistration;
use crate::facade::async_subscription::Weak;
use crate::facade::async_subscription::internal::session_lease::SessionLease;
use crate::facade::async_subscription::internal::session_slot::SessionSlot;
use crate::spi::ShutdownMode;

/// Coordinates exclusive access to one paused or running session.
pub(in crate::facade) struct AsyncSubscriptionControl<T: 'static> {
    /// Shared stop state observed by the session.
    pub(in crate::facade::async_subscription) signals: Arc<SessionSignals>,
    /// Session lease and disposal state.
    pub(super) slot: Mutex<SessionSlot<T>>,
    /// Wakes callers waiting for the session lease.
    pub(super) available: AsyncSignal,
    /// Canonical provider receiver close failure.
    pub(super) close_error: Mutex<Option<Arc<crate::error::SubscriptionCloseFailure>>>,
    /// Weak owner used to unregister this control from the bus.
    pub(in crate::facade::async_subscription) bus: Weak<AsyncEventBusInner>,
    /// Bus-local subscription identity.
    pub(super) id: Id,
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
    pub(in crate::facade::async_subscription) fn new(
        session: AsyncSession<T>,
        signals: Arc<SessionSignals>,
    ) -> Arc<Self> {
        let bus = Arc::downgrade(&session.inner);
        let id = session.id;
        Arc::new(Self {
            signals,
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
    pub(in crate::facade::async_subscription) async fn lease(&self) -> Option<SessionLease<'_, T>> {
        let registration = SignalRegistration::new(&self.available);
        let session = std::future::poll_fn(|cx| {
            let mut slot = self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if slot.disposed {
                return std::task::Poll::Ready(None);
            }
            if !slot.active
                && let Some(session) = slot.session.take()
            {
                slot.active = true;
                return std::task::Poll::Ready(Some(session));
            }
            registration.register(cx.waker());
            std::task::Poll::Pending
        })
        .await;
        session.map(|session| SessionLease {
            control: self,
            session: Some(session),
        })
    }
}

impl<T: 'static> AsyncSubscriptionControl<T> {
    /// Stops the subscription and relinquishes its receiver on handle drop.
    ///
    /// This synchronous disposal cannot await provider close; bus shutdown or
    /// explicit [`AsyncSubscription::close`] performs asynchronous cleanup.
    pub(in crate::facade::async_subscription) fn dispose(&self) {
        self.signals.stop(ShutdownMode::Immediate);
        let session = {
            let mut slot = self.slot.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            slot.disposed = true;
            if slot.active { None } else { slot.session.take() }
        };
        if let Some(bus) = self.bus.upgrade() {
            bus.controls
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&self.id);
        }
        drop(session);
        self.available.notify();
    }
}

impl<T: Send + Sync + 'static> AsyncShutdownDriver for AsyncSubscriptionControl<T> {
    /// Stops the active or future runner.
    fn stop(&self, mode: ShutdownMode) {
        self.signals.stop(mode);
    }

    /// Returns the canonical receiver close failure, when one exists.
    fn close_error(&self) -> Option<Arc<crate::error::SubscriptionCloseFailure>> {
        self.close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Stores the first close failure for later shutdown callers.
    ///
    /// # Parameters
    /// - `failure`: receiver close failure observed by a caller.
    ///
    /// # Returns
    /// The first stored failure.
    fn store_close_error(
        &self,
        failure: Arc<crate::error::SubscriptionCloseFailure>,
    ) -> Arc<crate::error::SubscriptionCloseFailure> {
        let mut stored = self
            .close_error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stored.get_or_insert(failure).clone()
    }

    /// Stops the session, resumes its runner if needed, and closes its
    /// receiver.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<crate::error::SubscriptionCloseFailure>>> + Send + 'a>> {
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
