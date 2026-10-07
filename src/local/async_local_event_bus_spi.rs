// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous in-process transport SPI.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use qubit_clock::StdTimer;
use qubit_clock::Timer;

use super::LocalEventBusConfig;
use super::async_local_event_subscription::AsyncLocalEventSubscription;
use super::internal::AsyncLocalShared;
use super::internal::AsyncMailbox;
use super::internal::MailboxKey;
use super::local_event_bus_spi::missing_native_payload_weight_error;
use super::local_event_bus_spi::operation_error;
use super::state::LocalEvent;
use super::state::LocalQueue;
use super::state::LocalQueueState;
use crate::error::ConfigurationError;
use crate::error::SpiError;
use crate::model::AdmissionStatus;
use crate::model::DestinationAdmission;
use crate::model::PublishAcknowledgement;
use crate::model::StartPosition;
use crate::model::SubscriptionDurability;
use crate::spi::AsyncEventBusSpi;
use crate::spi::AsyncEventSubscriptionSpi;
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
use crate::spi::SubscriptionModes;
use crate::spi::TransportPayload;

/// Asynchronous native-only local backend. Receive waits are driven by wakers;
/// the provider creates no receiver thread per subscription.
///
/// # Examples
///
/// ```
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use qubit_event_bus::registry::AsyncEventBusRegistry;
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let registry = AsyncEventBusRegistry::with_local()?;
/// let bus = registry.create(&EventBusConfig::default()).await?;
/// bus.shutdown(ShutdownMode::Immediate).await?;
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
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` if either configured
    /// capacity is zero.
    pub fn new(config: &LocalEventBusConfig) -> Result<Self, ConfigurationError> {
        Self::with_timer(config, Arc::new(StdTimer::new()))
    }

    /// Creates an async local SPI with an injected timer for deterministic
    /// tests.
    ///
    /// # Parameters
    /// - `config`: local provider queue limits.
    /// - `timer`: runtime-neutral timer used for graceful shutdown.
    ///
    /// # Returns
    /// An async local SPI using the supplied timer.
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` when either configured
    /// capacity is zero.
    pub fn with_timer(
        config: &LocalEventBusConfig,
        timer: Arc<dyn Timer>,
    ) -> Result<Self, ConfigurationError> {
        config.validate()?;
        Ok(Self {
            shared: Arc::new(AsyncLocalShared::new(
                config.get_queue_capacity(),
                config.get_max_total_outstanding(),
                config.get_max_total_outstanding_weight_bytes(),
                timer,
            )),
        })
    }
}

impl AsyncEventBusSpi for AsyncLocalEventBusSpi {
    /// Reports the local provider's native, ephemeral, per-key capabilities.
    ///
    /// # Returns
    /// An immutable capability set matching the in-process mailbox behavior.
    #[inline]
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::AcceptRetryReject,
            OrderingCapability::PerKey,
            DelayedDeliveryCapability::Native,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    /// Broadcasts a native event to every currently indexed topic mailbox.
    ///
    /// # Parameters
    /// - `message`: validated transport event to enqueue.
    ///
    /// # Returns
    /// Per-subscription admission results, including an empty list when no
    /// mailbox is subscribed.
    ///
    /// # Errors
    /// Returns an SPI error for encoded payloads, closed state, type conflicts,
    /// an unrepresentable delay deadline, or a missing native weight
    /// declaration when weight budgeting is enabled.
    fn publish<'a>(
        &'a self,
        message: OutboundMessage,
    ) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let topic = message.topic().clone();
            if !matches!(message.payload(), TransportPayload::Native(_)) {
                return Err(operation_error(
                    "publish",
                    Some(topic.as_str()),
                    "unsupported_payload_mode",
                ));
            }
            let mut event = LocalEvent::transport(topic.clone(), &message).ok_or_else(|| {
                operation_error("publish", Some(topic.as_str()), "delay_deadline_overflow")
            })?;
            let payload_type = match message.payload() {
                TransportPayload::Native(payload) => payload.as_ref().type_id(),
                TransportPayload::Encoded(_) => unreachable!("encoded payload was rejected above"),
            };
            let mailboxes = {
                let bus = self
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if bus.closed {
                    return Err(operation_error(
                        "publish",
                        Some(topic.as_str()),
                        "provider_closed",
                    ));
                }
                if bus
                    .payload_types
                    .get(&topic)
                    .is_some_and(|previous| *previous != payload_type)
                {
                    return Err(operation_error(
                        "publish",
                        Some(topic.as_str()),
                        "topic_type_conflict",
                    ));
                }
                bus.mailboxes_for_topic(&topic)
            };
            if self.shared.outstanding.requires_weight() {
                if message.native_payload_weight_bytes().is_none() {
                    return Err(missing_native_payload_weight_error(topic.as_str()));
                }
            } else {
                event.weight_bytes = 0;
            }
            let mut admissions = Vec::with_capacity(mailboxes.len());
            for mailbox in mailboxes {
                let mut queue = mailbox.queue.lock();
                let status = if queue.closed {
                    AdmissionStatus::Rejected("subscription is closed".into())
                } else if queue.pending_count() + queue.in_flight.len() >= mailbox.queue.capacity {
                    AdmissionStatus::Rejected("subscription queue is full".into())
                } else if !self.shared.outstanding.try_acquire(event.weight_bytes) {
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

    /// Creates an ephemeral mailbox for one supported subscription request.
    ///
    /// # Parameters
    /// - `request`: provider-facing subscription identity, topic, and options.
    ///
    /// # Returns
    /// A single-owner asynchronous receiver backed by the new mailbox.
    ///
    /// # Errors
    /// Returns an SPI error for unsupported subscription options, closed state,
    /// topic payload type conflicts, or duplicate subscription IDs.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let topic = request.topic().clone();
            if request.durability() != SubscriptionDurability::Ephemeral
                || request.group().is_some()
                || !matches!(request.start_position(), StartPosition::New)
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
            let mut bus = self
                .shared
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if bus.closed {
                return Err(operation_error(
                    "subscribe",
                    Some(topic.as_str()),
                    "provider_closed",
                ));
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
            )) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }

    /// Closes admission, waits according to the requested mode, and drains all
    /// registered mailboxes.
    ///
    /// # Parameters
    /// - `mode`: immediate close or graceful wait deadline.
    ///
    /// # Returns
    /// The provider shutdown outcome, cached across repeated calls.
    ///
    /// # Errors
    /// Returns an SPI error if the injected timer fails during graceful wait.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async move {
            {
                let mut bus = self
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                bus.closed = true;
            }
            let result = match mode {
                ShutdownMode::Immediate => ShutdownOutcome::Complete,
                ShutdownMode::Graceful { timeout } => {
                    let started = Instant::now();
                    let mut wait_registration = None;
                    let mut timer_future = None;
                    poll_fn(|cx| {
                        let bus = self
                            .shared
                            .state
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner);
                        let busy = bus.mailboxes.values().any(|mailbox| {
                            let queue = mailbox.queue.lock();
                            !queue.is_pending_empty() || !queue.in_flight.is_empty()
                        });
                        if !busy {
                            return Poll::Ready(Ok(ShutdownOutcome::Complete));
                        }
                        let remaining = if timeout == Duration::MAX {
                            None
                        } else {
                            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                                return Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            };
                            if remaining.is_zero() {
                                return Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            }
                            Some(remaining)
                        };
                        if let (None, Some(remaining)) = (timer_future.as_ref(), remaining) {
                            let timer = Arc::clone(&self.shared.timer);
                            timer_future = Some(Box::pin(async move {
                                let timer = timer.after(remaining).map_err(|error| {
                                    SpiError::Operation {
                                        provider_id: "local".into(),
                                        operation: "shutdown",
                                        resource: None,
                                        kind: "timer_error",
                                        retryable: Some(false),
                                        source: Box::new(error),
                                    }
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
                                as Pin<Box<dyn Future<Output = Result<(), SpiError>> + Send>>);
                        }
                        wait_registration = Some(self.shared.changed.register(cx.waker()));
                        drop(bus);
                        if let Some(timer) = timer_future.as_mut()
                            && let Poll::Ready(result) = timer.as_mut().poll(cx)
                        {
                            return Poll::Ready(result.map(|()| ShutdownOutcome::TimedOut));
                        }
                        Poll::Pending
                    })
                    .await?
                }
            };
            let (mailboxes, discarded) = {
                let mut bus = self
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if let Some(previous) = bus.outcome {
                    return Ok(previous);
                }
                bus.closed = true;
                let mailboxes = bus.drain_mailboxes();
                let mut discarded = Vec::new();
                for mailbox in &mailboxes {
                    let mut queue = mailbox.queue.lock();
                    queue.closed = true;
                    let removed = queue.clear_pending();
                    let weight = removed.iter().map(|event| event.weight_bytes).sum();
                    self.shared.outstanding.release(removed.len(), weight);
                    discarded.extend(removed);
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
        let discarded = queue.clear_pending();
        let weight = discarded.iter().map(|event| event.weight_bytes).sum();
        shared.outstanding.release(discarded.len(), weight);
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
