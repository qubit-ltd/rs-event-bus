// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async subscription session close and delivery abandonment.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::super::AsyncSubscriptionControl;
use crate::Diagnostic;
use crate::facade::async_event_bus::catch_spi_future;
use crate::facade::async_subscription::AsyncSession;
use crate::facade::async_subscription::BusState;
use crate::facade::async_subscription::SubscriptionCloseErrors;
use crate::facade::async_subscription::internal::owned_delivery_task::discard_unstarted_tasks;
use crate::spi::ShutdownMode;

impl<T: Send + Sync + 'static> AsyncSession<T> {
    /// Stops this receiver and asynchronously releases provider resources.
    ///
    /// # Parameters
    /// - `control`: coordinator retaining close failures for bus shutdown.
    ///
    /// # Returns
    /// `Ok(())` after close succeeds or when the bus already closed.
    ///
    /// # Errors
    /// Returns a lifecycle error containing the provider receiver close
    /// failure.
    pub(in crate::facade) async fn close(
        &mut self,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), crate::error::LifecycleError> {
        if *self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == BusState::Closed
        {
            return Ok(());
        }
        self.signals.stop(ShutdownMode::Immediate);
        if let Some(handler) = self.handler.clone()
            && let Err(error) = self.run_loop(handler).await
        {
            self.inner.emit(&Diagnostic::InternalFailure {
                origin: "close_resume".into(),
                message: error.to_string().into(),
            });
        }
        match self.close_inner(control).await {
            Ok(()) => Ok(()),
            Err(failure) => Err(crate::error::LifecycleError::SubscriptionClose(Arc::new(
                SubscriptionCloseErrors::from_failures(vec![failure]),
            ))),
        }
    }

    /// Closes the provider receiver and unregisters it after successful close.
    ///
    /// # Parameters
    /// - `control`: owner that stores the canonical close failure.
    ///
    /// # Returns
    /// Success or the canonical provider close failure.
    ///
    /// # Errors
    /// Returns the canonical failure recorded for this receiver's close.
    pub(in crate::facade) async fn close_inner(
        &mut self,
        control: &AsyncSubscriptionControl<T>,
    ) -> Result<(), Arc<crate::error::SubscriptionCloseFailure>> {
        self.signals.stop(ShutdownMode::Immediate);
        self.admission_waiter.take();
        let _close = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (*state == BusState::Running).then(|| self.inner.tracker.close_started())
        };
        let close_result = {
            if let Some(receiver) = self.receiver.as_mut() {
                let close = crate::spi::panic_boundary::catch_spi_call(
                    self.inner.provider_id.as_str(),
                    "close",
                    Some(self.subscriber_id.as_str()),
                    || receiver.close(),
                );
                match close {
                    Ok(future) => {
                        catch_spi_future(
                            future,
                            &self.inner.provider_id,
                            "close",
                            Some(self.subscriber_id.as_str()),
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                Ok(())
            }
        };
        if let Err(error) = close_result {
            let failure = self.inner.record_close_error(control, &self.subscriber_id, error);
            return Err(failure);
        }
        self.receiver.take();
        self.inner
            .controls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
        Ok(())
    }

    /// Records one facade-owned delivery abandoned by an ephemeral provider.
    ///
    /// This updates the bus shutdown report count only when provider durability
    /// does not promise recovery.
    pub(in crate::facade) fn record_abandoned_delivery(&self) {
        if self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
            self.inner.abandoned_deliveries.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Drops the current unstarted or unsettled delivery during Immediate stop.
    pub(in crate::facade) fn abandon_pending(&mut self) {
        if self.pending.take().is_some() {
            self.record_abandoned_delivery();
        }
    }

    /// Drops queued handler futures that Immediate stop forbids from starting.
    pub(in crate::facade) fn discard_unstarted_tasks(&mut self) {
        let ephemeral = self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral;
        discard_unstarted_tasks(&mut self.tasks, &self.inner.abandoned_deliveries, ephemeral);
    }
}
