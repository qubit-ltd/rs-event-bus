// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Owned receiver and delivery state for one asynchronous subscription.

use std::collections::VecDeque;
use std::sync::Arc;

use qubit_id::Id;

use super::super::super::async_event_bus::AsyncEventBusInner;
use super::OwnedDeliveryTask;
use super::PendingDelivery;
use super::SessionSignals;
use crate::facade::async_subscription::SharedAsyncHandler;
use crate::model::SubscribeOptions;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::spi::AsyncEventSubscriptionSpi;

/// Runtime state for one typed subscription, including the receiver and all
/// delivery futures that have been accepted by the facade.
///
/// # Type Parameters
/// - `T`: message type accepted by this subscription's topic and handler.
pub(in crate::facade) struct AsyncSession<T: 'static> {
    /// Shared bus lifecycle, tracker, provider, and pipelines.
    pub(in crate::facade) inner: Arc<AsyncEventBusInner>,
    /// Subscription counters retained independently by the public handle.
    pub(in crate::facade) metrics: Arc<crate::facade::internal::DeliveryMetrics>,
    /// Bus-local subscription identity.
    pub(in crate::facade) id: Id,
    /// Logical subscriber identity used in SPI diagnostics.
    pub(in crate::facade) subscriber_id: SubscriberId,
    /// Typed topic receiving provider messages.
    pub(in crate::facade) topic: Topic<T>,
    /// Codec retained for the subscription lifetime, when configured.
    pub(in crate::facade) codec: Option<Arc<dyn crate::codec::EventCodec<T>>>,
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
// Drives the caller-owned subscription run loop.
mod runner;

// Applies ordered settlement attempts and terminal failure cleanup.
mod settlement;
// Defines receiver-owner events selected by the poll loop.
mod runner_event;

impl<T: 'static> Drop for AsyncSession<T> {
    /// Releases metadata after actual payload and future owners have been
    /// dropped.
    fn drop(&mut self) {
        self.inner.scheduler.stop_subscription(self.id);
        let abandoned =
            self.buffered.len() + self.tasks.len() + self.completed.len() + self.completed_during_settlement.len();
        if self.inner.capabilities.durability() == crate::spi::DurabilityCapability::Ephemeral {
            self.inner
                .abandoned_deliveries
                .fetch_add(abandoned as u64, std::sync::atomic::Ordering::AcqRel);
            for _ in 0..abandoned {
                self.metrics.record_abandoned_ephemeral();
            }
        }
        self.buffered.clear();
        self.tasks.clear();
        self.completed.clear();
        self.completed_during_settlement.clear();
        let _ = self.inner.scheduler.unregister(self.id);
        self.inner.notify_scheduler();
    }
}
