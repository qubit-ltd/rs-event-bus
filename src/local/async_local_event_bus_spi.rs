// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous in-process transport SPI.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Instant;

use super::LocalEventBusConfig;
use super::async_local_event_subscription::AsyncLocalEventSubscription;
use super::async_signal::AsyncSignal;
use super::local_event_bus_spi::operation_error;
use super::state::LocalEvent;
use super::state::LocalQueue;
use super::state::LocalQueueState;
use crate::error::SpiError;
use crate::model::AdmissionStatus;
use crate::model::DestinationAdmission;
use crate::model::PublishAcknowledgement;
use crate::model::SubscriberId;
use crate::spi::AsyncEventBusSpi;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::OrderingCapability;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiFuture;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MailboxKey {
    topic: TopicAddress,
    subscriber: SubscriberId,
}

pub(super) struct AsyncMailbox {
    pub(super) queue: Arc<LocalQueue>,
}

#[derive(Default)]
struct AsyncBusState {
    closed: bool,
    outcome: Option<ShutdownOutcome>,
    mailboxes: HashMap<MailboxKey, Arc<AsyncMailbox>>,
    payload_types: HashMap<TopicAddress, TypeId>,
}

pub(super) struct AsyncLocalShared {
    capacity: usize,
    pub(super) outstanding: super::outstanding_budget::OutstandingBudget,
    state: Mutex<AsyncBusState>,
    pub(super) changed: AsyncSignal,
    pub(super) timer: Arc<dyn qubit_clock::Timer>,
}

/// Asynchronous native-only local backend. Receive waits are driven by wakers;
/// the provider creates no receiver thread per subscription.
pub struct AsyncLocalEventBusSpi {
    pub(super) shared: Arc<AsyncLocalShared>,
}

impl AsyncLocalEventBusSpi {
    /// Creates an async local SPI from validated transport settings.
    pub fn new(config: &LocalEventBusConfig) -> Result<Self, crate::error::ConfigurationError> {
        config.validate()?;
        Ok(Self::with_timer(config, Arc::new(qubit_clock::StdTimer::new())))
    }

    /// Creates an async local SPI with an injected timer for deterministic
    /// tests.
    pub fn with_timer(config: &LocalEventBusConfig, timer: Arc<dyn qubit_clock::Timer>) -> Self {
        Self {
            shared: Arc::new(AsyncLocalShared {
                capacity: config.get_queue_capacity(),
                outstanding: super::outstanding_budget::OutstandingBudget::new(config.get_max_total_outstanding()),
                state: Mutex::new(AsyncBusState::default()),
                changed: AsyncSignal::default(),
                timer,
            }),
        }
    }
}

impl AsyncEventBusSpi for AsyncLocalEventBusSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::Native,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let topic = message.topic().clone();
            if !matches!(message.payload(), TransportPayload::Native(_)) {
                return Err(operation_error(
                    "publish",
                    Some(topic.as_str()),
                    "unsupported_payload_mode",
                ));
            }
            let event = LocalEvent::transport(topic.clone(), &message)
                .ok_or_else(|| operation_error("publish", Some(topic.as_str()), "delay_deadline_overflow"))?;
            let payload_type = match message.payload() {
                TransportPayload::Native(payload) => payload.as_ref().type_id(),
                TransportPayload::Encoded(_) => unreachable!("encoded payload was rejected above"),
            };
            let mailboxes = {
                let bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                if bus.closed {
                    return Err(operation_error("publish", Some(topic.as_str()), "provider_closed"));
                }
                if bus
                    .payload_types
                    .get(&topic)
                    .is_some_and(|previous| *previous != payload_type)
                {
                    return Err(operation_error("publish", Some(topic.as_str()), "topic_type_conflict"));
                }
                bus.mailboxes
                    .iter()
                    .filter(|(key, _)| key.topic == topic)
                    .map(|(_, mailbox)| mailbox.clone())
                    .collect::<Vec<_>>()
            };
            let mut admissions = Vec::with_capacity(mailboxes.len());
            for mailbox in mailboxes {
                let mut queue = mailbox.queue.lock();
                let status = if queue.closed {
                    AdmissionStatus::Rejected("subscription is closed".into())
                } else if queue.pending_count() + queue.in_flight.len() >= mailbox.queue.capacity {
                    AdmissionStatus::Rejected("subscription queue is full".into())
                } else if !self.shared.outstanding.try_acquire() {
                    AdmissionStatus::Rejected("provider outstanding capacity is full".into())
                } else {
                    queue.enqueue_back(event.clone());
                    AdmissionStatus::Accepted
                };
                drop(queue);
                if status == AdmissionStatus::Accepted {
                    mailbox.queue.async_ready.notify_all();
                }
                admissions.push(DestinationAdmission::new(
                    mailbox.queue.id,
                    mailbox.queue.subscriber_id.clone(),
                    status,
                ));
            }
            if !admissions.is_empty() {
                self.shared.changed.notify_all();
            }
            Ok(PublishAcknowledgement::DestinationAdmissions(admissions))
        })
    }

    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn crate::spi::AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let topic = request.topic().clone();
            if request.durability() != crate::model::SubscriptionDurability::Ephemeral
                || request.group().is_some()
                || !matches!(request.start_position(), crate::model::StartPosition::New)
            {
                return Err(operation_error(
                    "subscribe",
                    Some(topic.as_str()),
                    "unsupported_subscription_options",
                ));
            }
            let key = MailboxKey {
                topic: topic.clone(),
                subscriber: request.subscriber_id().clone(),
            };
            let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if bus.closed {
                return Err(operation_error("subscribe", Some(topic.as_str()), "provider_closed"));
            }
            if bus
                .payload_types
                .get(&topic)
                .is_some_and(|previous| *previous != request.payload_type_id())
            {
                return Err(operation_error(
                    "subscribe",
                    Some(topic.as_str()),
                    "topic_type_conflict",
                ));
            }
            if bus.mailboxes.contains_key(&key) {
                return Err(operation_error(
                    "subscribe",
                    Some(topic.as_str()),
                    "duplicate_subscriber",
                ));
            }
            let id = request.subscription_id();
            let queue = Arc::new(LocalQueue {
                id,
                topic: topic.clone(),
                subscriber_id: request.subscriber_id().clone(),
                capacity: self.shared.capacity,
                state: Mutex::new(LocalQueueState::default()),
                ready: Default::default(),
                async_ready: Default::default(),
            });
            let mailbox = Arc::new(AsyncMailbox { queue });
            bus.mailboxes.insert(key, mailbox.clone());
            bus.payload_types.insert(topic, request.payload_type_id());
            drop(bus);
            Ok(Box::new(AsyncLocalEventSubscription::new(
                Arc::clone(&self.shared),
                mailbox,
                request.subscription_id(),
            )) as Box<dyn crate::spi::AsyncEventSubscriptionSpi>)
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async move {
            {
                let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                bus.closed = true;
            }
            let result = match mode {
                ShutdownMode::Immediate => ShutdownOutcome::Complete,
                ShutdownMode::Graceful { timeout } => {
                    let started = Instant::now();
                    let mut wait_registration = None;
                    let mut timer_future = None;
                    std::future::poll_fn(|cx| {
                        let bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                        let busy = bus.mailboxes.values().any(|mailbox| {
                            let queue = mailbox.queue.lock();
                            !queue.is_pending_empty() || !queue.in_flight.is_empty()
                        });
                        if !busy {
                            return std::task::Poll::Ready(Ok(ShutdownOutcome::Complete));
                        }
                        let remaining = if timeout == std::time::Duration::MAX {
                            None
                        } else {
                            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                                return std::task::Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            };
                            if remaining.is_zero() {
                                return std::task::Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            }
                            Some(remaining)
                        };
                        if let (None, Some(remaining)) = (timer_future.as_ref(), remaining) {
                            let timer = Arc::clone(&self.shared.timer);
                            timer_future = Some(Box::pin(async move {
                                let timer = timer.after(remaining).map_err(|error| SpiError::Operation {
                                    provider_id: "local".into(),
                                    operation: "shutdown",
                                    resource: None,
                                    kind: "timer_error",
                                    retryable: Some(false),
                                    source: Box::new(error),
                                })?;
                                timer.await.map_err(|error| SpiError::Operation {
                                    provider_id: "local".into(),
                                    operation: "shutdown",
                                    resource: None,
                                    kind: "timer_error",
                                    retryable: Some(false),
                                    source: Box::new(error),
                                })
                            })
                                as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), SpiError>> + Send>>);
                        }
                        wait_registration = Some(self.shared.changed.register(cx.waker()));
                        drop(bus);
                        if let Some(timer) = timer_future.as_mut()
                            && let std::task::Poll::Ready(result) = std::future::Future::poll(timer.as_mut(), cx)
                        {
                            return std::task::Poll::Ready(result.map(|()| ShutdownOutcome::TimedOut));
                        }
                        std::task::Poll::Pending
                    })
                    .await?
                }
            };
            let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(previous) = bus.outcome {
                return Ok(previous);
            }
            bus.closed = true;
            let mailboxes = std::mem::take(&mut bus.mailboxes).into_values().collect::<Vec<_>>();
            for mailbox in &mailboxes {
                let mut queue = mailbox.queue.lock();
                queue.closed = true;
                let released = queue.pending_count() + queue.in_flight.len();
                queue.clear_pending();
                queue.in_flight.clear();
                self.shared.outstanding.release(released);
                drop(queue);
                mailbox.queue.async_ready.notify_all();
            }
            bus.payload_types.clear();
            bus.outcome = Some(result);
            drop(bus);
            self.shared.changed.notify_all();
            Ok(result)
        })
    }
}

/// Closes and unregisters one ephemeral mailbox, discarding all unsettled work.
///
/// The bus lock linearizes close against subscribe and route lookup; the queue
/// lock then prevents a publisher holding an earlier route snapshot from
/// adding work after the mailbox has closed. Repeated calls are harmless.
pub(super) fn close_mailbox(shared: &AsyncLocalShared, mailbox: &Arc<AsyncMailbox>) {
    let key = MailboxKey {
        topic: mailbox.queue.topic.clone(),
        subscriber: mailbox.queue.subscriber_id.clone(),
    };
    let mut bus = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
    let is_current_mailbox = bus
        .mailboxes
        .get(&key)
        .is_some_and(|current| Arc::ptr_eq(current, mailbox));
    {
        let mut queue = mailbox.queue.lock();
        queue.closed = true;
        let released = queue.pending_count() + queue.in_flight.len();
        queue.clear_pending();
        queue.in_flight.clear();
        shared.outstanding.release(released);
    }
    if is_current_mailbox {
        bus.mailboxes.remove(&key);
        if !bus.mailboxes.keys().any(|candidate| candidate.topic == key.topic) {
            bus.payload_types.remove(&key.topic);
        }
    }
    drop(bus);
    mailbox.queue.async_ready.notify_all();
    shared.changed.notify_all();
}
