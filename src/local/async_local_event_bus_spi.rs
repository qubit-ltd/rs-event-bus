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
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::MonotonicInstant;
use qubit_clock::StdTimer;
use qubit_clock::Timer;

use super::LocalCapacityMonitor;
use super::LocalEventBusConfig;
use super::async_local_event_subscription::AsyncLocalEventSubscription;
use super::internal::AsyncBusState;
use super::internal::AsyncLocalShared;
use super::internal::AsyncMailbox;
use super::internal::MailboxKey;
use super::local_event_bus_spi::missing_native_payload_weight_error;
use super::local_event_bus_spi::operation_error;
use super::local_event_bus_spi::pre_admission_publish_error;
use super::state::LocalEvent;
use super::state::LocalQueue;
use super::state::LocalQueueState;
use crate::error::ConfigurationError;
use crate::error::SpiError;
use crate::model::AdmissionRejectionKind;
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
/// Delayed publications validate known admission rejections before registering
/// the deadline with the configured timer, then recheck provider state after
/// registration succeeds. A timer registration failure therefore does not
/// mask a known rejection. Pre-admission timer failures preserve their
/// `qubit_clock::TimeError` source and report
/// [`crate::model::PublishEffect::NotAccepted`].
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

/// Builds a pre-admission publish error while preserving its clock failure.
///
/// # Parameters
/// - `resource`: optional topic associated with the publication.
/// - `kind`: stable provider error classification.
/// - `source`: original timer failure.
///
/// # Returns
/// A non-retryable publish error that records that no destination accepted the
/// message.
fn pre_admission_publish_time_error(
    resource: Option<&str>,
    kind: &'static str,
    source: qubit_clock::TimeError,
) -> SpiError {
    SpiError::Publish {
        provider_id: "local".into(),
        resource: resource.map(Into::into),
        kind,
        retryable: Some(false),
        effect: crate::model::PublishEffect::NotAccepted,
        source: Box::new(source),
    }
}

impl AsyncLocalEventBusSpi {
    /// Creates a monitor sharing this provider's outstanding-capacity
    /// accounting.
    ///
    /// The handle observes shared state rather than capturing a snapshot.
    /// Retaining it does not keep the provider open or reserve capacity for
    /// later publication. It can still read retained counters after
    /// provider shutdown.
    ///
    /// # Returns
    /// A monitor for the provider-wide budget and configured per-subscription
    /// limit.
    pub(crate) fn capacity_monitor(&self) -> LocalCapacityMonitor {
        LocalCapacityMonitor::new(Arc::clone(&self.shared.outstanding), self.shared.capacity())
    }

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
    /// - `timer`: runtime-neutral timer whose clock domain drives async receive
    ///   timeouts, delayed delivery, and graceful shutdown deadlines.
    ///
    /// # Returns
    /// An async local SPI using the supplied timer.
    ///
    /// # Errors
    /// Returns `ConfigurationError::InvalidField` when either configured
    /// capacity is zero.
    pub fn with_timer(config: &LocalEventBusConfig, timer: Arc<dyn Timer>) -> Result<Self, ConfigurationError> {
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
    /// mailbox is subscribed. A mailbox or provider that closes during
    /// admission rejects only that target, so the result may show partial
    /// acceptance.
    ///
    /// # Errors
    /// Returns an SPI error for encoded payloads, a provider closed before
    /// routing, type conflicts,
    /// an unrepresentable delay deadline, a timer registration failure, or a
    /// missing native weight declaration when weight budgeting is enabled.
    /// Known admission rejections take precedence over timer registration. A
    /// future delay is validated before admission; an unrepresentable deadline
    /// yields `delay_deadline_overflow`, while other registration failures
    /// yield `timer_error`. Both preserve the underlying time error and report
    /// [`crate::model::PublishEffect::NotAccepted`]. Provider state is checked
    /// again after successful registration. Receive-side waiting may fail
    /// independently later.
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move {
            let topic = message.topic().clone();
            if !matches!(message.payload(), TransportPayload::Native(_)) {
                return Err(pre_admission_publish_error(
                    Some(topic.as_str()),
                    "unsupported_payload_mode",
                ));
            }
            let now_instant = self.shared.timer.clock().now();
            let now = now_instant.elapsed_since_origin();
            let mut event = LocalEvent::<Duration>::transport_at(topic.clone(), &message, now).ok_or_else(|| {
                pre_admission_publish_time_error(
                    Some(topic.as_str()),
                    "delay_deadline_overflow",
                    qubit_clock::TimeError::InstantOverflow,
                )
            })?;
            let payload_type = match message.payload() {
                TransportPayload::Native(payload) => payload.as_ref().type_id(),
                TransportPayload::Encoded(_) => unreachable!("encoded payload was rejected above"),
            };
            let validate_admission = |bus: &AsyncBusState| {
                if self.shared.closed.load(Ordering::Acquire) {
                    return Err(pre_admission_publish_error(Some(topic.as_str()), "provider_closed"));
                }
                if bus
                    .payload_types
                    .get(&topic)
                    .is_some_and(|previous| *previous != payload_type)
                {
                    return Err(pre_admission_publish_error(Some(topic.as_str()), "topic_type_conflict"));
                }
                if self.shared.outstanding.requires_weight() && message.native_payload_weight_bytes().is_none() {
                    return Err(missing_native_payload_weight_error(topic.as_str()));
                }
                Ok(())
            };
            if let Some(deadline) = event.not_before
                && deadline > now
            {
                {
                    let bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                    validate_admission(&bus)?;
                }
                let deadline = MonotonicInstant::new(now_instant.domain(), deadline);
                drop(self.shared.timer.at(deadline).map_err(|error| {
                    let kind = if matches!(error, qubit_clock::TimeError::InstantOverflow) {
                        "delay_deadline_overflow"
                    } else {
                        "timer_error"
                    };
                    pre_admission_publish_time_error(Some(topic.as_str()), kind, error)
                })?);
            }
            let mailboxes = {
                let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                validate_admission(&bus)?;
                bus.publish_mailboxes_for_topic(&topic)
            };
            if !self.shared.outstanding.requires_weight() {
                event.weight_bytes = 0;
            }
            let mut admissions = Vec::with_capacity(mailboxes.len());
            for mailbox in mailboxes {
                let mut queue = mailbox.queue.lock();
                let rejection = if queue.closed {
                    Some((AdmissionRejectionKind::SubscriptionClosed, "subscription is closed"))
                } else if self.shared.closed.load(Ordering::Acquire) {
                    Some((AdmissionRejectionKind::SubscriptionClosed, "provider is closing"))
                } else if queue.pending_count() + queue.in_flight.len() >= mailbox.queue.capacity {
                    Some((
                        AdmissionRejectionKind::SubscriptionCapacity,
                        "subscription queue is full",
                    ))
                } else {
                    self.shared
                        .outstanding
                        .try_reserve(event.weight_bytes)
                        .err()
                        .map(|kind| (kind, "provider outstanding capacity is full"))
                };
                if let Some((
                    kind @ (AdmissionRejectionKind::SubscriptionClosed | AdmissionRejectionKind::SubscriptionCapacity),
                    _,
                )) = rejection
                {
                    self.shared.outstanding.record_rejection(kind);
                }
                let status = if let Some((_, reason)) = rejection {
                    AdmissionStatus::Rejected(reason.into())
                } else {
                    queue.enqueue_back_at(event.clone(), now);
                    AdmissionStatus::Accepted
                };
                drop(queue);
                if status == AdmissionStatus::Accepted {
                    mailbox.queue.async_ready.notify_all();
                }
                admissions.push(if let Some((kind, reason)) = rejection {
                    DestinationAdmission::rejected(mailbox.queue.id, mailbox.queue.subscriber_id.clone(), kind, reason)
                } else {
                    DestinationAdmission::new(mailbox.queue.id, mailbox.queue.subscriber_id.clone(), status)
                });
            }
            if !admissions.is_empty() {
                self.shared.changed.notify_all();
            }
            admissions.sort_unstable_by_key(DestinationAdmission::subscription_id);
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
            let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if self.shared.closed.load(Ordering::Acquire) {
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
            let queue = Arc::new(LocalQueue::<Duration> {
                id,
                topic: topic.clone(),
                subscriber_id: request.subscriber_id().clone(),
                capacity: self.shared.capacity(),
                state: Mutex::new(LocalQueueState::<Duration>::default()),
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
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async move {
            {
                let _bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                self.shared.closed.store(true, Ordering::Release);
            }
            let result = match mode {
                ShutdownMode::Immediate => ShutdownOutcome::Complete,
                ShutdownMode::Graceful { timeout } => {
                    let started = self.shared.timer.clock().now();
                    let mut wait_registration = None;
                    let mut timer_future = None;
                    poll_fn(|cx| {
                        let next_registration = self.shared.changed.register(cx.waker());
                        let busy = {
                            let bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                            bus.mailboxes.values().any(|mailbox| {
                                let queue = mailbox.queue.lock();
                                !queue.is_pending_empty() || !queue.in_flight.is_empty()
                            })
                        };
                        let retired = wait_registration.replace(next_registration);
                        drop(retired);
                        if !busy {
                            return Poll::Ready(Ok(ShutdownOutcome::Complete));
                        }
                        let now = self.shared.timer.clock().now();
                        let elapsed = now.duration_since(started);
                        let remaining = if timeout == Duration::MAX {
                            None
                        } else {
                            let Ok(elapsed) = elapsed else {
                                return Poll::Ready(Err(operation_error("shutdown", None, "clock_error")));
                            };
                            let Some(remaining) = timeout.checked_sub(elapsed) else {
                                return Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            };
                            if remaining.is_zero() {
                                return Poll::Ready(Ok(ShutdownOutcome::TimedOut));
                            }
                            Some(remaining)
                        };
                        if let (None, Some(remaining)) = (timer_future.as_ref(), remaining) {
                            let timer = Arc::clone(&self.shared.timer);
                            let deadline = now.checked_add(remaining);
                            timer_future = Some(Box::pin(async move {
                                let deadline = deadline.map_err(|error| SpiError::Operation {
                                    provider_id: "local".into(),
                                    operation: "shutdown",
                                    resource: None,
                                    kind: "timer_error",
                                    retryable: Some(false),
                                    source: Box::new(error),
                                })?;
                                let timer = timer.at(deadline).map_err(|error| SpiError::Operation {
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
                                as Pin<Box<dyn Future<Output = Result<(), SpiError>> + Send>>);
                        }
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
                let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                if let Some(previous) = bus.outcome {
                    return Ok(previous);
                }
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
            for mailbox in mailboxes {
                mailbox.queue.async_ready.notify_all();
            }
            self.shared.changed.notify_all();
            drop(discarded);
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
    mailbox.queue.async_ready.notify_all();
    shared.changed.notify_all();
    drop(discarded);
}
