// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous in-process transport SPI.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Instant;

use qubit_clock::StdTimer;
use qubit_clock::Timer;

use super::LocalEventBusConfig;
use super::async_local_event_subscription::AsyncLocalEventSubscription;
use super::internal::AsyncLocalShared;
use super::internal::AsyncMailbox;
use super::internal::LocalEvent;
use super::internal::LocalQueue;
use super::internal::LocalQueueState;
use super::internal::MailboxKey;
use super::local_event_bus_spi::operation_error;
use crate::error::SpiError;
use crate::model::AdmissionStatus;
use crate::model::DestinationAdmission;
use crate::model::PublishAcknowledgement;
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
use crate::spi::TransportPayload;

/// Asynchronous native-only local backend. Receive waits are driven by wakers;
/// the provider creates no receiver thread per subscription.
///
/// # Examples
///
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::{AsyncEventBusRegistry, EventBusConfig};
///
/// let registry = AsyncEventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default()).await?;
/// bus.shutdown(qubit_event_bus::spi::ShutdownMode::Immediate).await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncLocalEventBusSpi {
    /// Queues and timer shared by async provider operations.
    pub(super) shared: Arc<AsyncLocalShared>,
}

impl AsyncLocalEventBusSpi {
    /// Creates an async local SPI from validated transport settings.
    ///
    /// # Parameters
    /// - `config`: validated local provider queue limits.
    ///
    /// # Returns
    /// An async local SPI using the standard timer.
    pub fn new(config: &LocalEventBusConfig) -> Result<Self, crate::error::ConfigurationError> {
        Self::with_timer(config, Arc::new(StdTimer::new()))
    }

    /// Creates an async local SPI with an injected timer for deterministic
    /// tests.
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` when either configured
    /// capacity is zero.
    ///
    /// # Parameters
    /// - `config`: local provider queue limits.
    /// - `timer`: runtime-neutral timer used for graceful shutdown.
    ///
    /// # Returns
    /// An async local SPI using the supplied timer.
    pub fn with_timer(
        config: &LocalEventBusConfig,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, crate::error::ConfigurationError> {
        config.validate()?;
        Ok(Self {
            shared: Arc::new(AsyncLocalShared::new(
                config.get_queue_capacity(),
                config.get_max_total_outstanding(),
                timer,
            )),
        })
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
            crate::spi::SubscriptionModes::EPHEMERAL,
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
                bus.mailboxes_for_topic(&topic)
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
                subscription_id: request.subscription_id(),
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
            let id = request.subscription_id();
            let queue = Arc::new(LocalQueue {
                id,
                topic: topic.clone(),
                subscriber_id: request.subscriber_id().clone(),
                capacity: self.shared.capacity(),
                state: Mutex::new(LocalQueueState::default()),
                ready: Default::default(),
                async_ready: Default::default(),
            });
            let mailbox = Arc::new(AsyncMailbox { queue });
            if !bus.insert_mailbox(key, mailbox.clone()) {
                return Err(operation_error(
                    "subscribe",
                    Some(topic.as_str()),
                    "duplicate_subscription",
                ));
            }
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
            let (mailboxes, discarded) = {
                let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(previous) = bus.outcome {
                    return Ok(previous);
                }
                bus.closed = true;
                let mailboxes = bus.drain_mailboxes();
                let mut discarded = Vec::new();
                for mailbox in &mailboxes {
                    let mut queue = mailbox.queue.lock();
                    queue.closed = true;
                    let released = queue.pending_count() + queue.in_flight.len();
                    discarded.extend(queue.clear_pending());
                    self.shared.outstanding.release(released);
                }
                bus.payload_types.clear();
                bus.outcome = Some(result);
                (mailboxes, discarded)
            };
            drop(discarded);
            for mailbox in mailboxes {
                mailbox.queue.async_ready.notify_all();
            }
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
///
/// # Parameters
/// - `shared`: async provider state that owns the mailbox.
/// - `mailbox`: subscription mailbox to close and remove.
pub(super) fn close_mailbox(shared: &AsyncLocalShared, mailbox: &Arc<AsyncMailbox>) {
    let key = MailboxKey {
        subscription_id: mailbox.queue.id,
    };
    let topic = mailbox.queue.topic.clone();
    let discarded = {
        let mut bus = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut queue = mailbox.queue.lock();
        queue.closed = true;
        let released = queue.pending_count() + queue.in_flight.len();
        let discarded = queue.clear_pending();
        shared.outstanding.release(released);
        drop(queue);
        if bus.remove_mailbox_if_same(key, mailbox) && !bus.has_topic(&topic) {
            bus.payload_types.remove(&topic);
        }
        discarded
    };
    drop(discarded);
    mailbox.queue.async_ready.notify_all();
    shared.changed.notify_all();
}
