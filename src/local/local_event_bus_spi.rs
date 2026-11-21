// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous in-process transport SPI.

use std::any::TypeId;
use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::LocalCapacityMonitor;
use super::LocalEventBusConfig;
use super::local_event_subscription::LocalEventSubscription;
use super::state::LocalEvent;
use super::state::LocalQueue;
use super::state::LocalQueueState;
use super::state::LocalSharedState;
use crate::error::ConfigurationError;
use crate::error::SpiError;
use crate::internal::finite_wait::FiniteWait;
use crate::model::AdmissionRejectionKind;
use crate::model::AdmissionStatus;
use crate::model::DestinationAdmission;
use crate::model::PublishAcknowledgement;
use crate::model::PublishEffect;
use crate::model::StartPosition;
use crate::model::SubscriptionDurability;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OrderingCapability;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::SubscriptionModes;
use crate::spi::TopicAddress;
use crate::spi::TransportPayload;

/// Synchronous local backend with one bounded queue per subscription.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::registry::EventBusConfig;
/// use qubit_event_bus::registry::EventBusRegistry;
/// use qubit_event_bus::model::PublishRequest;
/// use qubit_event_bus::model::Topic;
/// use qubit_event_bus::spi::ShutdownMode;
///
/// let registry = EventBusRegistry::with_local().unwrap();
/// let bus = registry.create(&EventBusConfig::default()).unwrap();
/// let topic = Topic::<u32>::new("orders.created").unwrap();
/// let receipt = bus.publish(PublishRequest::new(topic, 42).unwrap()).unwrap();
/// assert!(!receipt.acknowledgement().is_dropped());
/// bus.shutdown(ShutdownMode::Immediate).unwrap();
/// ```
pub struct LocalEventBusSpi {
    /// Queues and provider-wide shutdown state shared by local SPI handles.
    pub(super) shared: Arc<LocalSharedState>,
}

impl LocalEventBusSpi {
    /// Wakes synchronous receivers for one topic without changing queue state.
    ///
    /// # Parameters
    /// - `topic`: Topic whose waiting receivers should be notified.
    #[cfg(test)]
    pub(crate) fn wake_topic_waiters_for_test(&self, topic: &TopicAddress) {
        let queues = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .live_queues_for_topic(topic);
        for queue in queues {
            queue.ready.notify_all();
        }
    }

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
        LocalCapacityMonitor::new(Arc::clone(&self.shared.outstanding), self.shared.capacity)
    }

    /// Creates an SPI instance from local transport configuration.
    ///
    /// # Parameters
    /// - `config`: validated local provider queue limits.
    ///
    /// # Returns
    /// A synchronous local provider SPI with empty queues.
    ///
    /// # Errors
    /// Returns [`ConfigurationError::InvalidField`] if either
    /// capacity is zero.
    pub fn new(config: &LocalEventBusConfig) -> Result<Self, ConfigurationError> {
        config.validate()?;
        Ok(Self {
            shared: LocalSharedState::new(
                config.get_queue_capacity(),
                config.get_max_total_outstanding(),
                config.get_max_total_outstanding_weight_bytes(),
            ),
        })
    }
}

impl EventBusSpi for LocalEventBusSpi {
    /// Blocks until this topic has no queued or unsettled local deliveries.
    ///
    /// # Parameters
    /// - `topic`: topic whose live subscription queues are observed.
    /// - `timeout`: total wait budget, or `None` to wait without a deadline.
    ///
    /// # Returns
    /// `Ok(Some(true))` when idle, or `Ok(Some(false))` after the timeout.
    /// This local implementation always supports the wait and never returns
    /// `None` or a provider error.
    ///
    /// # Errors
    /// This implementation never returns a provider error.
    fn wait_for_topic_idle(&self, topic: &TopicAddress, timeout: Option<Duration>) -> Result<Option<bool>, SpiError> {
        let budget = timeout.map(FiniteWait::new);
        loop {
            let queues;
            let (version, busy) = {
                let mut bus = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                queues = bus.live_queues_for_topic(topic);
                let mut empty_queues = Vec::with_capacity(queues.len());
                let mut busy = false;
                for queue in &queues {
                    let queue_state = queue.lock();
                    if !queue_state.is_pending_empty() || !queue_state.in_flight.is_empty() {
                        busy = true;
                        break;
                    }
                    empty_queues.push(queue_state);
                    #[cfg(test)]
                    crate::internal::test_wait_probe::announce("topic_idle_after_queue_check");
                }
                (bus.change_version, busy)
            };
            // Keep the temporary queue owners alive until all queue guards and
            // the bus-state guard have been released.
            drop(queues);
            if !busy {
                return Ok(Some(true));
            }
            let Some(budget) = &budget else {
                let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                if state.change_version != version {
                    continue;
                }
                drop(self.shared.changed.wait(state).unwrap_or_else(PoisonError::into_inner));
                continue;
            };
            let Some(remaining) = budget.remaining() else {
                return Ok(Some(false));
            };
            let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if state.change_version != version {
                continue;
            }
            #[cfg(test)]
            crate::internal::test_wait_probe::announce("topic_idle");
            let (_state, _) = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Declares native, ephemeral delivery with per-key ordering and
    /// settlement.
    ///
    /// # Returns
    /// The local provider's fixed capability declaration.
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

    /// Shares a native event with each matching bounded subscription queue.
    ///
    /// # Parameters
    /// - `message`: event to validate and enqueue without copying its payload.
    ///
    /// # Returns
    /// Per-destination admission outcomes; a full or closed target queue, or
    /// a provider that begins closing during admission, rejects only that
    /// target. A publish spanning shutdown may therefore be partially
    /// accepted.
    ///
    /// # Errors
    /// Returns a non-retryable operation error for an encoded payload, an
    /// overflowing delay deadline, a provider closed before routing, a topic
    /// type conflict, or a missing native weight declaration when weight
    /// budgeting is enabled. Provider closure after routing is reported as a
    /// per-destination `SubscriptionClosed` rejection and can produce partial
    /// acceptance.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let topic = message.topic().clone();
        validate_message(&message, &topic)?;
        let mut event = LocalEvent::transport(topic.clone(), &message)
            .ok_or_else(|| pre_admission_publish_error(Some(topic.as_str()), "delay_deadline_overflow"))?;
        let payload_type_id = native_payload_type_id(message.payload());
        let queues = {
            let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if self.shared.closed.load(Ordering::Acquire) {
                return Err(pre_admission_publish_error(Some(topic.as_str()), "provider_closed"));
            }
            let queues = state.live_queues_for_topic(&topic);
            let bucket = state.topics.get(&topic);
            if bucket.is_some_and(|bucket| bucket.payload_type_id.is_some_and(|type_id| type_id != payload_type_id)) {
                return Err(pre_admission_publish_error(Some(topic.as_str()), "topic_type_conflict"));
            }
            if self.shared.outstanding.requires_weight() && message.native_payload_weight_bytes().is_none() {
                return Err(missing_native_payload_weight_error(topic.as_str()));
            }
            let mut queues = queues;
            if let Some(bucket) = state.topics.get_mut(&topic) {
                bucket.rotate_publish_queues(&mut queues);
            }
            queues
        };
        #[cfg(test)]
        crate::internal::test_wait_probe::announce("publish_after_route_snapshot");
        if !self.shared.outstanding.requires_weight() {
            event.weight_bytes = 0;
        }
        let mut admissions = Vec::new();
        let mut admitted_any = false;
        for queue in queues {
            if queue.topic != topic {
                continue;
            }
            let mut state = queue.lock();
            let rejection = if state.closed {
                Some((AdmissionRejectionKind::SubscriptionClosed, "subscription is closed"))
            } else if self.shared.closed.load(Ordering::Acquire) {
                Some((AdmissionRejectionKind::SubscriptionClosed, "provider is closing"))
            } else if state.pending_count() + state.in_flight.len() >= queue.capacity {
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
                state.enqueue_back(event.clone());
                queue.ready.notify_one();
                admitted_any = true;
                AdmissionStatus::Accepted
            };
            drop(state);
            admissions.push(if let Some((kind, reason)) = rejection {
                DestinationAdmission::rejected(queue.id, queue.subscriber_id.clone(), kind, reason)
            } else {
                DestinationAdmission::new(queue.id, queue.subscriber_id.clone(), status)
            });
            #[cfg(test)]
            crate::internal::test_wait_probe::announce("publish_between_targets");
        }
        if admitted_any {
            signal_changed(&self.shared);
        }
        admissions.sort_unstable_by_key(DestinationAdmission::subscription_id);
        Ok(PublishAcknowledgement::DestinationAdmissions(admissions))
    }

    /// Registers an empty, ephemeral native queue for an independent
    /// subscriber.
    ///
    /// # Parameters
    /// - `request`: receiver identity, topic, payload type, and transport
    ///   options.
    ///
    /// # Returns
    /// The single-owner receiver for the newly registered queue.
    ///
    /// # Errors
    /// Returns an operation error for a closed provider, unsupported
    /// durability, group or start position, a topic type conflict, or a
    /// duplicate live ID.
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let id = request.subscription_id();
        let queue = Arc::new(LocalQueue {
            id,
            topic: request.topic().clone(),
            subscriber_id: request.subscriber_id().clone(),
            capacity: self.shared.capacity,
            state: Mutex::new(LocalQueueState::default()),
            ready: Default::default(),
            async_ready: Default::default(),
        });
        let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
        if self.shared.closed.load(Ordering::Acquire) {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "provider_closed",
            ));
        }
        if request.durability() != SubscriptionDurability::Ephemeral
            || request.group().is_some()
            || !matches!(request.start_position(), StartPosition::New)
        {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "unsupported_subscription_options",
            ));
        }
        let topic = request.topic().clone();
        let live_queues = state.live_queues_for_topic(&topic);
        let has_type_conflict = state
            .topics
            .get(&topic)
            .and_then(|bucket| bucket.payload_type_id)
            .is_some_and(|type_id| type_id != request.payload_type_id());
        if has_type_conflict {
            return Err(operation_error(
                "subscribe",
                Some(request.topic().as_str()),
                "topic_type_conflict",
            ));
        }
        if state.subscription_ids.contains(&id) {
            let is_live = state
                .topics
                .values()
                .find_map(|bucket| bucket.queues.get(&id).and_then(Weak::upgrade))
                .is_some();
            if is_live {
                return Err(operation_error(
                    "subscribe",
                    Some(request.topic().as_str()),
                    "duplicate_subscription",
                ));
            }
            state.remove_stale_id(id);
        }
        let bucket = state.topics.entry(topic).or_default();
        bucket.payload_type_id = Some(request.payload_type_id());
        bucket.queues.insert(id, Arc::downgrade(&queue));
        state.subscription_ids.insert(id);
        drop(state);
        drop(live_queues);
        Ok(Box::new(LocalEventSubscription::new(self.shared.clone(), queue)))
    }

    /// Closes admission and discards remaining queues after the drain period.
    ///
    /// Calls serialize on the shutdown gate and reuse the first completed
    /// outcome. Waiting for an earlier caller's gate is outside this caller's
    /// graceful timeout budget. Closing wakes synchronous and async receivers.
    /// The cached outcome is published after queue cleanup and before discarded
    /// payloads are destroyed. Repeated or reentrant calls may return that
    /// outcome while another caller is still running payload destructors;
    /// shutdown is not a cross-caller barrier for user destructors.
    ///
    /// # Parameters
    /// - `mode`: immediate close, or a bounded wait for queued and unsettled
    ///   work. An immediate request upgrades a graceful shutdown already in
    ///   progress and wakes its wait; all callers then reuse the cached result.
    ///
    /// # Returns
    /// `Ok(Complete)` after draining or immediate close, or `Ok(TimedOut)`
    /// after an incomplete graceful drain. This implementation returns no
    /// SPI error.
    ///
    /// # Errors
    /// This implementation does not return an SPI error.
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        if mode == ShutdownMode::Immediate {
            let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(outcome) = state.shutdown_outcome {
                return Ok(outcome);
            }
            if !state.shutdown_immediate_requested {
                state.shutdown_immediate_requested = true;
                state.change_version = state.change_version.wrapping_add(1);
                drop(state);
                self.shared.changed.notify_all();
                #[cfg(test)]
                crate::internal::test_wait_probe::announce("shutdown_immediate_registered");
            }
        }
        let shutdown_gate = self.shared.shutdown_gate.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(outcome) = self
            .shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .shutdown_outcome
        {
            return Ok(outcome);
        }
        {
            let _state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            self.shared.closed.store(true, Ordering::Release);
        }
        signal_changed(&self.shared);

        let mut outcome = match mode {
            ShutdownMode::Immediate => ShutdownOutcome::Complete,
            ShutdownMode::Graceful { timeout } => {
                let budget = FiniteWait::new(timeout);
                loop {
                    let (version, immediate_requested, queues) = {
                        let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                        let version = state.change_version;
                        let immediate_requested = state.shutdown_immediate_requested;
                        let queues = state.live_queues();
                        (version, immediate_requested, queues)
                    };
                    if immediate_requested {
                        break ShutdownOutcome::Complete;
                    }
                    let busy = queues.iter().any(|queue| {
                        let state = queue.lock();
                        !state.is_pending_empty() || !state.in_flight.is_empty()
                    });
                    if !busy {
                        break ShutdownOutcome::Complete;
                    }
                    let Some(remaining) = budget.remaining() else {
                        break ShutdownOutcome::TimedOut;
                    };
                    let state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                    if state.shutdown_immediate_requested {
                        break ShutdownOutcome::Complete;
                    }
                    if state.change_version != version {
                        continue;
                    }
                    #[cfg(test)]
                    crate::internal::test_wait_probe::announce("graceful_shutdown");
                    let (_state, _) = self
                        .shared
                        .changed
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            }
        };

        if matches!(mode, ShutdownMode::Graceful { .. }) {
            #[cfg(test)]
            crate::internal::test_wait_probe::announce("graceful_shutdown_decision");
        }

        let queues = {
            let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.live_queues()
        };
        let mut discarded = Vec::new();
        for queue in queues {
            let mut state = queue.lock();
            state.closed = true;
            let pending = state.clear_pending();
            let weight = pending.iter().map(|event| event.weight_bytes).sum();
            self.shared.outstanding.release(pending.len(), weight);
            drop(state);
            queue.ready.notify_all();
            discarded.extend(pending);
        }
        #[cfg(test)]
        crate::internal::test_wait_probe::announce("shutdown_before_outcome_cache");
        {
            let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
            if matches!(mode, ShutdownMode::Graceful { .. }) && state.shutdown_immediate_requested {
                outcome = ShutdownOutcome::Complete;
            }
            state.shutdown_outcome = Some(outcome);
        }
        signal_changed(&self.shared);
        drop(shutdown_gate);
        drop(discarded);
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::PoisonError;
    use crate::internal::test_wait_probe;
    use crate::local::LocalEventBusConfig;
    use crate::local::LocalEventBusSpi;
    use crate::model::AdmissionStatus;
    use crate::model::PublishAcknowledgement;
    use crate::spi::DeliveryDisposition;
    use crate::spi::EventBusSpi;
    use crate::spi::ReceiveOutcome;
    use crate::spi::ShutdownMode;
    use crate::spi::ShutdownOutcome;
    use crate::spi::TopicAddress;

    #[test]
    fn test_local_topic_idle_requires_consistent_queue_snapshot() {
        crate::tests::large_timeout_wait_tests::isolated(
            "test_local_topic_idle_requires_consistent_queue_snapshot",
            "topic-idle-consistent-snapshot",
            || {
                let topic = "probe.topic.consistent-snapshot";
                let spi = Arc::new(LocalEventBusSpi::new(&LocalEventBusConfig::new().queue_capacity(1)).unwrap());
                let mut receiver_a = spi
                    .subscribe(crate::tests::large_timeout_wait_tests::subscription(101, topic))
                    .unwrap();
                let mut receiver_b = spi
                    .subscribe(crate::tests::large_timeout_wait_tests::subscription(102, topic))
                    .unwrap();
                crate::tests::large_timeout_wait_tests::publish(&*spi, topic, "snapshot-first");

                let ReceiveOutcome::Message(message_a) = receiver_a.receive(Duration::ZERO).unwrap() else {
                    panic!("first queue receives the initial message");
                };
                receiver_a
                    .settle(message_a.settlement().unwrap(), DeliveryDisposition::Accept)
                    .unwrap();
                drop(message_a);

                let queues = spi
                    .shared
                    .state
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .live_queues_for_topic(&TopicAddress::new(topic).unwrap());
                let queue_a = Arc::clone(&queues[0]);
                drop(queues);
                let (scan_gate, _, release_scan) =
                    test_wait_probe::install_gated("topic_idle_after_queue_check", "unused_topic_idle_signal");
                let wait_spi = Arc::clone(&spi);
                let waiter = thread::spawn(move || {
                    wait_spi
                        .wait_for_topic_idle(&TopicAddress::new(topic).unwrap(), Some(Duration::ZERO))
                        .unwrap()
                });
                scan_gate
                    .recv_timeout(Duration::from_secs(2))
                    .expect("scan checked the first empty queue");

                let lock_was_retained = match queue_a.state.try_lock() {
                    Ok(guard) => {
                        drop(guard);
                        let PublishAcknowledgement::DestinationAdmissions(admissions) =
                            crate::tests::large_timeout_wait_tests::publish_ack(&*spi, topic, "snapshot-second")
                        else {
                            panic!("local publish reports destination admissions");
                        };
                        assert_eq!(&AdmissionStatus::Accepted, admissions[0].status());
                        assert!(matches!(admissions[1].status(), AdmissionStatus::Rejected(_)));
                        let ReceiveOutcome::Message(message_b) = receiver_b.receive(Duration::ZERO).unwrap() else {
                            panic!("second queue retains its first delivery");
                        };
                        receiver_b
                            .settle(message_b.settlement().unwrap(), DeliveryDisposition::Accept)
                            .unwrap();
                        drop(message_b);
                        false
                    }
                    Err(_) => true,
                };
                release_scan.send(()).unwrap();
                assert_eq!(Some(false), waiter.join().unwrap());
                assert!(
                    lock_was_retained,
                    "an empty queue lock remains held through the idle decision"
                );

                if lock_was_retained {
                    let ReceiveOutcome::Message(message_b) = receiver_b.receive(Duration::ZERO).unwrap() else {
                        panic!("second queue retains its first delivery");
                    };
                    receiver_b
                        .settle(message_b.settlement().unwrap(), DeliveryDisposition::Accept)
                        .unwrap();
                    drop(message_b);
                    let PublishAcknowledgement::DestinationAdmissions(admissions) =
                        crate::tests::large_timeout_wait_tests::publish_ack(&*spi, topic, "snapshot-second")
                    else {
                        panic!("local publish reports destination admissions");
                    };
                    assert!(
                        admissions
                            .iter()
                            .all(|admission| admission.status() == &AdmissionStatus::Accepted)
                    );
                }
                assert_eq!(
                    Some(false),
                    spi.wait_for_topic_idle(&TopicAddress::new(topic).unwrap(), Some(Duration::ZERO))
                        .unwrap()
                );
                let ReceiveOutcome::Message(message_a) = receiver_a.receive(Duration::ZERO).unwrap() else {
                    panic!("second message is pending for the first queue");
                };
                receiver_a
                    .settle(message_a.settlement().unwrap(), DeliveryDisposition::Accept)
                    .unwrap();
                drop(message_a);
                if let ReceiveOutcome::Message(message_b) = receiver_b.receive(Duration::ZERO).unwrap() {
                    receiver_b
                        .settle(message_b.settlement().unwrap(), DeliveryDisposition::Accept)
                        .unwrap();
                    drop(message_b);
                }
                assert_eq!(
                    Some(true),
                    spi.wait_for_topic_idle(&TopicAddress::new(topic).unwrap(), Some(Duration::ZERO))
                        .unwrap()
                );
            },
        );
    }

    #[test]
    fn test_local_immediate_escalates_graceful_during_cleanup() {
        crate::tests::large_timeout_wait_tests::isolated(
            "test_local_immediate_escalates_graceful_during_cleanup",
            "immediate-escalates-graceful-during-cleanup",
            || {
                let topic = "probe.shutdown.cleanup-escalation";
                let spi = Arc::new(LocalEventBusSpi::new(&LocalEventBusConfig::default()).unwrap());
                let capacity = spi.capacity_monitor();
                let mut receiver = spi
                    .subscribe(crate::tests::large_timeout_wait_tests::subscription(103, topic))
                    .unwrap();
                crate::tests::large_timeout_wait_tests::publish(&*spi, topic, "cleanup-escalation");
                let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).unwrap() else {
                    panic!("published message is received");
                };

                let (cache_gate, immediate_registered, release_cache) =
                    test_wait_probe::install_gated("shutdown_before_outcome_cache", "shutdown_immediate_registered");
                let graceful_spi = Arc::clone(&spi);
                let (graceful_tx, graceful_rx) = mpsc::channel();
                let graceful = thread::spawn(move || {
                    graceful_tx
                        .send(graceful_spi.shutdown(ShutdownMode::Graceful {
                            timeout: Duration::ZERO,
                        }))
                        .unwrap();
                });
                cache_gate
                    .recv_timeout(Duration::from_secs(2))
                    .expect("cleanup completed before caching outcome");

                let immediate_spi = Arc::clone(&spi);
                let (immediate_tx, immediate_rx) = mpsc::channel();
                let immediate = thread::spawn(move || {
                    immediate_tx
                        .send(immediate_spi.shutdown(ShutdownMode::Immediate))
                        .unwrap();
                });
                immediate_registered
                    .recv_timeout(Duration::from_secs(2))
                    .expect("Immediate registered before cache");
                release_cache.send(()).unwrap();

                assert_eq!(
                    ShutdownOutcome::Complete,
                    graceful_rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap()
                );
                assert_eq!(
                    ShutdownOutcome::Complete,
                    immediate_rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap()
                );
                assert_eq!(
                    ShutdownOutcome::Complete,
                    spi.shutdown(ShutdownMode::Immediate).unwrap()
                );
                assert!(matches!(
                    receiver.receive(Duration::ZERO).unwrap(),
                    ReceiveOutcome::Closed
                ));
                drop(message);
                assert_eq!(0, capacity.snapshot().outstanding_count);
                immediate.join().unwrap();
                graceful.join().unwrap();
            },
        );
    }
}

/// Rejects SPI payload modes that are outside the local provider capability.
///
/// # Parameters
/// - `message`: outbound message to inspect.
/// - `topic`: destination used to contextualize failures.
///
/// # Returns
/// `Ok(())` for a native payload.
///
/// # Errors
/// Returns an unsupported-payload-mode SPI error for encoded payloads.
fn validate_message(message: &OutboundMessage, topic: &TopicAddress) -> Result<(), SpiError> {
    match message.payload() {
        TransportPayload::Native(_) => Ok(()),
        TransportPayload::Encoded(_) => Err(pre_admission_publish_error(
            Some(topic.as_str()),
            "unsupported_payload_mode",
        )),
    }
}

/// Returns the concrete Rust type carried by a validated native payload.
///
/// # Parameters
/// - `payload`: payload previously checked against local provider capabilities.
///
/// # Returns
/// The concrete native payload `TypeId`.
///
/// # Panics
/// Panics if the payload is encoded, which indicates that local payload-mode
/// validation was skipped.
#[must_use]
fn native_payload_type_id(payload: &TransportPayload) -> TypeId {
    match payload {
        TransportPayload::Native(value) => value.as_ref().type_id(),
        TransportPayload::Encoded(_) => unreachable!("local payload mode validation runs first"),
    }
}

/// Builds a provider-tagged operation error with optional topic context.
///
/// # Parameters
/// - `operation`: SPI operation name.
/// - `resource`: optional topic or resource identity.
/// - `kind`: stable provider error kind.
///
/// # Returns
/// A non-retryable operation error tagged with provider ID `local`.
pub(super) fn operation_error(operation: &'static str, resource: Option<&str>, kind: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "local".into(),
        operation,
        resource: resource.map(Into::into),
        kind,
        retryable: Some(false),
        source: Box::new(IoError::other(kind)) as Box<dyn Error + Send + Sync>,
    }
}

/// Builds a local publish error for a failure before any destination is
/// admitted.
///
/// # Parameters
/// - `resource`: optional topic or resource identity.
/// - `kind`: stable provider error kind.
///
/// # Returns
/// A non-retryable publish error that explicitly proves no destination was
/// admitted.
pub(super) fn pre_admission_publish_error(resource: Option<&str>, kind: &'static str) -> SpiError {
    SpiError::Publish {
        provider_id: "local".into(),
        resource: resource.map(Into::into),
        kind,
        retryable: Some(false),
        effect: PublishEffect::NotAccepted,
        source: Box::new(IoError::other(kind)) as Box<dyn Error + Send + Sync>,
    }
}

/// Builds the stable local error for a missing required native payload weight.
///
/// # Parameters
/// - `topic`: destination topic that omitted its required weight.
///
/// # Returns
/// A non-retryable pre-admission publish error for that topic.
pub(super) fn missing_native_payload_weight_error(topic: &str) -> SpiError {
    pre_admission_publish_error(Some(topic), "missing_native_payload_weight")
}

/// Builds a provider-tagged invalid-token failure with its stable reason.
///
/// # Parameters
/// - `resource`: optional subscription or topic identity.
/// - `reason`: stable invalid-token reason.
///
/// # Returns
/// A non-retryable invalid-settlement-token error.
pub(super) fn invalid_token_error(resource: Option<&str>, reason: &'static str) -> SpiError {
    SpiError::InvalidSettlementToken {
        provider_id: "local".into(),
        operation: "settle",
        resource: resource.map(Into::into),
        reason,
        retryable: Some(false),
        source: Box::new(IoError::other(reason)),
    }
}

/// Advances the provider change generation before waking graceful shutdown.
///
/// # Parameters
/// - `shared`: provider state whose waiters must be notified.
pub(super) fn signal_changed(shared: &LocalSharedState) {
    let mut state = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
    state.change_version = state.change_version.wrapping_add(1);
    drop(state);
    shared.changed.notify_all();
}

#[cfg(test)]
mod capacity_rejection_tests {
    use std::any::TypeId;
    use std::sync::Arc;

    use super::LocalEventBusConfig;
    use super::LocalEventBusSpi;
    use crate::model::AdmissionRejectionKind;
    use crate::model::EventId;
    use crate::model::ProviderOptions;
    use crate::model::PublishAcknowledgement;
    use crate::model::StartPosition;
    use crate::model::SubscriberId;
    use crate::model::SubscriptionDurability;
    use crate::spi::EventBusSpi;
    use crate::spi::OutboundMessage;
    use crate::spi::SpiSubscriptionRequest;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    #[test]
    fn test_closed_subscription_publish_increments_monitor_counter() {
        let config = LocalEventBusConfig::new();
        let bus = LocalEventBusSpi::new(&config).expect("valid local config");
        let monitor = bus.capacity_monitor();
        let topic = TopicAddress::new("local.closed-counter").expect("valid topic");
        let request = SpiSubscriptionRequest::new(
            qubit_id::Id::new(1),
            topic.clone(),
            SubscriberId::new("closed-counter").expect("valid subscriber"),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<u32>(),
        );
        let _subscription = bus.subscribe(request).expect("subscription");
        let queue = {
            let mut state = bus.shared.state.lock().expect("bus state");
            let bucket = state.topics.get_mut(&topic).expect("topic registered");
            bucket.live_queues().into_iter().next().expect("live queue")
        };
        queue.lock().closed = true;

        let message = OutboundMessage::new(
            topic,
            EventId::new("closed-counter-event").expect("valid event ID"),
            std::time::SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u32)),
        );
        let acknowledgement = bus.publish(message).expect("publish acknowledgement");
        let PublishAcknowledgement::DestinationAdmissions(admissions) = acknowledgement else {
            panic!("local publish returns destination admissions");
        };
        assert_eq!(admissions.len(), 1);
        assert_eq!(
            admissions[0].rejection_kind(),
            Some(AdmissionRejectionKind::SubscriptionClosed)
        );
        assert_eq!(monitor.snapshot().rejected_subscription_closed, 1);
    }
}
