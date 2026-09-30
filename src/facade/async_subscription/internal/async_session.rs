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

use super::super::super::async_admission::AsyncAdmissionFuture;
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
/// - `T`: Payload type carried by the subscription. The `'static` bound allows
///   accepted delivery tasks to retain payloads after the caller-owned runner
///   pauses.
pub(in crate::facade) struct AsyncSession<T: 'static> {
    /// Shared bus lifecycle, tracker, provider, and pipelines.
    pub(in crate::facade) inner: Arc<AsyncEventBusInner>,
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
    /// Current delivery waiting for handler or settlement completion.
    pub(in crate::facade) pending: Option<PendingDelivery<T>>,
    /// Delivery held while another delivery owns admission.
    pub(in crate::facade) waiting_admission: Option<PendingDelivery<T>>,
    /// Handler futures already accepted by facade admission.
    pub(in crate::facade) tasks: Vec<OwnedDeliveryTask<T>>,
    /// Completed tasks waiting for terminal settlement.
    pub(in crate::facade) completed: VecDeque<PendingDelivery<T>>,
    /// Whether handler processing is waiting for a caller's handler result.
    pub(in crate::facade) defer_settlement: bool,
    /// Handler used to resume tasks after the runner future is dropped.
    pub(in crate::facade) handler: Option<SharedAsyncHandler<T>>,
    /// Pending bus-wide admission request retained across runner pauses.
    pub(in crate::facade) admission_waiter: Option<AsyncAdmissionFuture>,
}

// Closes and disposes the provider receiver.
mod close;
// Creates and completes owned delivery tasks.
mod delivery;
// Drives the caller-owned subscription run loop.
mod runner;
