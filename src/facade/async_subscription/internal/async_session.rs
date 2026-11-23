// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owned receiver and delivery state for one asynchronous subscription.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use qubit_id::Id;

use self::session_registration_cleanup::SessionRegistrationCleanup;
use super::super::super::async_event_bus::AsyncEventBusInner;
use super::OwnedDeliveryTask;
use super::PendingDelivery;
use super::SessionSignals;
use crate::codec::EventCodec;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::facade::async_subscription::with_subscription_scope;
use crate::facade::internal::DeliveryMetrics;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::spi::AsyncEventSubscriptionSpi;
use crate::spi::DurabilityCapability;

/// Runtime state for one typed subscription, including the receiver and all
/// delivery futures that have been accepted by the facade.
///
/// # Type Parameters
/// - `T`: message type accepted by this subscription's topic and handler.
pub(in crate::facade) struct AsyncSession<T: 'static> {
    /// Shared bus lifecycle, tracker, provider, and pipelines.
    pub(in crate::facade) inner: Arc<AsyncEventBusInner>,
    /// Subscription counters retained independently by the public handle.
    pub(in crate::facade) metrics: Arc<DeliveryMetrics>,
    /// Bus-local subscription identity.
    pub(in crate::facade) id: Id,
    /// Logical subscriber identity used in SPI diagnostics.
    pub(in crate::facade) subscriber_id: SubscriberId,
    /// Typed topic receiving provider messages.
    pub(in crate::facade) topic: Topic<T>,
    /// Codec retained for the subscription lifetime, when configured.
    pub(in crate::facade) codec: Option<Arc<dyn EventCodec<T>>>,
    /// Handler, retry, settlement, and provider policies.
    pub(in crate::facade) options: SubscribeOptions<T>,
    /// Single-owner receiver, retained until successful close.
    pub(in crate::facade) receiver: Option<Box<dyn AsyncEventSubscriptionSpi>>,
    /// Whether the provider has ended this receiver.
    pub(in crate::facade) receiver_closed: bool,
    /// Stop and wake signals shared with the public handle.
    pub(in crate::facade) signals: Arc<SessionSignals>,
    /// Queued deliveries which have not received a handler grant.
    pub(in crate::facade) buffered: VecDeque<PendingDelivery<T>>,
    /// Granted tasks retained across pauses.
    pub(in crate::facade) tasks: Vec<OwnedDeliveryTask<T>>,
    /// Handler-complete deliveries retaining their lanes through settlement.
    pub(in crate::facade) completed: VecDeque<PendingDelivery<T>>,
    /// Handler completions retained even if an in-flight settlement is
    /// cancelled.
    pub(in crate::facade) completed_during_settlement: VecDeque<PendingDelivery<T>>,
    /// Handler used to resume tasks after the runner future is dropped.
    pub(in crate::facade) handler: Option<SharedAsyncHandler<T>>,
}

// Closes and disposes the provider receiver.
mod close;
// Creates and completes owned delivery tasks.
mod delivery;
// Polls handler futures while retaining their duration guard.
mod handler_duration_future;
// Drives the caller-owned subscription run loop.
mod runner;

// Applies ordered settlement attempts and terminal failure cleanup.
mod settlement;
// Defines receiver-owner events selected by the poll loop.
mod runner_event;
// Releases the scheduler registration after lease-owning session state.
mod session_registration_cleanup;

impl<T: 'static> Drop for AsyncSession<T> {
    /// Releases scheduler metadata after locally owned payloads and futures.
    ///
    /// Moving the lease-owning containers into a scope declared after the
    /// registration guard ensures they are dropped first, including during
    /// unwinding from a payload destructor.
    fn drop(&mut self) {
        let already_panicking = std::thread::panicking();
        self.inner.scheduler.stop_subscription(self.id);
        let abandoned =
            self.buffered.len() + self.tasks.len() + self.completed.len() + self.completed_during_settlement.len();
        if self.inner.capabilities.durability() == DurabilityCapability::Ephemeral {
            self.inner
                .abandoned_deliveries
                .fetch_add(abandoned as u64, Ordering::AcqRel);
            for _ in 0..abandoned {
                self.metrics.record_abandoned_ephemeral();
            }
        }
        let registration = SessionRegistrationCleanup::new(&self.inner, self.id);
        let bus_key = Arc::as_ptr(&self.inner) as usize;
        let subscription_id = self.id;
        let mut first_panic = None;
        macro_rules! release {
            ($value:expr) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    with_subscription_scope(bus_key, subscription_id, || drop($value));
                })) {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    } else {
                        crate::internal::panic_payload::dispose_panic_payload(payload);
                    }
                }
            };
        }
        macro_rules! release_delivery {
            ($delivery:expr) => {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    with_subscription_scope(bus_key, subscription_id, || $delivery.drop_independently());
                })) {
                    if first_panic.is_none() {
                        first_panic = Some(payload);
                    } else {
                        crate::internal::panic_payload::dispose_panic_payload(payload);
                    }
                }
            };
        }
        {
            let mut tasks = std::mem::take(&mut self.tasks);
            let mut buffered = std::mem::take(&mut self.buffered);
            let mut completed = std::mem::take(&mut self.completed);
            let mut completed_during_settlement = std::mem::take(&mut self.completed_during_settlement);
            while let Some(task) = tasks.pop() {
                release!(task);
            }
            while let Some(mut delivery) = buffered.pop_front() {
                self.emit_failure_diagnostic(
                    delivery.failure_diagnostic.take(),
                    delivery.event_id.clone(),
                    self.topic.name().into(),
                );
                release_delivery!(delivery);
            }
            while let Some(mut delivery) = completed.pop_front() {
                self.emit_failure_diagnostic(
                    delivery.failure_diagnostic.take(),
                    delivery.event_id.clone(),
                    self.topic.name().into(),
                );
                release_delivery!(delivery);
            }
            while let Some(mut delivery) = completed_during_settlement.pop_front() {
                self.emit_failure_diagnostic(
                    delivery.failure_diagnostic.take(),
                    delivery.event_id.clone(),
                    self.topic.name().into(),
                );
                release_delivery!(delivery);
            }
        }
        drop(registration);
        release!(self.receiver.take());
        release!(self.handler.take());
        if let Some(payload) = first_panic {
            if already_panicking {
                crate::internal::panic_payload::dispose_panic_payload(payload);
            } else {
                resume_unwind(payload);
            }
        }
    }
}
