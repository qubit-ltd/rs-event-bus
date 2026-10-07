// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-owner synchronous receiver for a local subscription.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use super::local_event_bus_spi::invalid_token_error;
use super::local_event_bus_spi::operation_error;
use super::local_event_bus_spi::signal_changed;
use super::state::LocalInFlight;
use super::state::LocalQueue;
use super::state::LocalSettlementHandle;
use super::state::LocalSettlementState;
use super::state::LocalSharedState;
use crate::error::SpiError;
use crate::spi::DeliveryDisposition;
use crate::spi::EventSubscriptionSpi;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;

/// Single-owner synchronous receiver for one local subscription.
#[must_use = "dropping the subscription closes it"]
pub(super) struct LocalEventSubscription {
    /// Shared bus state used for lifecycle notifications and unregistering.
    shared: Arc<LocalSharedState>,
    /// Per-subscription queue and settlement state.
    queue: Arc<LocalQueue>,
}

impl LocalEventSubscription {
    /// Creates a receiver bound to a registered local subscription queue.
    ///
    /// # Parameters
    /// - `shared`: local bus state shared by provider operations.
    /// - `queue`: registered queue consumed by this receiver.
    ///
    /// # Returns
    /// A single-owner subscription receiver.
    #[inline]
    pub(super) fn new(shared: Arc<LocalSharedState>, queue: Arc<LocalQueue>) -> Self {
        Self { shared, queue }
    }
}

impl EventSubscriptionSpi for LocalEventSubscription {
    /// Receives the next eligible queue head and retains its capacity until
    /// settled.
    ///
    /// # Parameters
    /// - `timeout`: maximum blocking wait; zero polls without waiting.
    ///
    /// # Returns
    /// A message with its token, `TimedOut` when no event becomes eligible
    /// within the budget, or `Closed` after this queue closes.
    ///
    /// # Errors
    /// Returns an operation error if the delivery token sequence is exhausted.
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let started = Instant::now();
        let mut state = self.queue.lock();
        loop {
            if state.closed {
                return Ok(ReceiveOutcome::Closed);
            }
            let now = Instant::now();
            if let Some(event) = state.pop_ready(now) {
                let Some(sequence) = state.next_delivery_token.checked_add(1) else {
                    state.enqueue_front(event);
                    return Err(operation_error(
                        "receive",
                        Some(self.queue.topic.as_str()),
                        "settlement_token_exhausted",
                    ));
                };
                state.next_delivery_token = sequence;
                let token = format!("{}:{sequence}", event.event_id()).into_boxed_str();
                let settlement = Arc::new(Mutex::new(LocalSettlementState {
                    token_id: token.clone(),
                    disposition: None,
                }));
                state.in_flight.insert(
                    token,
                    LocalInFlight {
                        event: event.clone(),
                        settlement: settlement.clone(),
                    },
                );
                let message = event.into_inbound(self.queue.id, settlement);
                drop(state);
                signal_changed(&self.shared);
                return Ok(ReceiveOutcome::Message(message));
            }
            if timeout.is_zero() {
                return Ok(ReceiveOutcome::TimedOut);
            }
            let Some(remaining) = timeout.checked_sub(started.elapsed()) else {
                return Ok(ReceiveOutcome::TimedOut);
            };
            if remaining.is_zero() {
                return Ok(ReceiveOutcome::TimedOut);
            }
            let delay = state.next_ready_delay(now);
            let wait_for = delay.filter(|delay| *delay < remaining).unwrap_or(remaining);
            let (next, result) = self
                .queue
                .ready
                .wait_timeout(state, wait_for)
                .unwrap_or_else(PoisonError::into_inner);
            state = next;
            if result.timed_out() && timeout.checked_sub(started.elapsed()).is_none() {
                return if state.closed {
                    Ok(ReceiveOutcome::Closed)
                } else {
                    Ok(ReceiveOutcome::TimedOut)
                };
            }
        }
    }

    /// Applies an idempotent settlement decision to an authentic local token.
    ///
    /// Accept and reject release capacity; retry requeues the original event
    /// at its lane's head while retaining its reservation and waking receivers.
    ///
    /// # Parameters
    /// - `token`: token issued by this receiver for an in-flight event.
    /// - `disposition`: accept, reject, or retry action to apply.
    ///
    /// # Returns
    /// `Ok(())` after the action or an identical repeated settlement.
    ///
    /// # Errors
    /// Returns an invalid-token error for a foreign, unknown, forged, or
    /// already settled token with a conflicting disposition.
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        if !token.belongs_to(self.queue.id) {
            return Err(invalid_token_error(
                Some(self.queue.topic.as_str()),
                "foreign_subscription",
            ));
        }
        let settlement = token
            .downcast_ref::<LocalSettlementHandle>()
            .ok_or_else(|| invalid_token_error(Some(self.queue.topic.as_str()), "unknown_token"))?
            .clone();
        let mut state = self.queue.lock();
        let mut token_state = settlement.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(previous) = token_state.disposition {
            return if previous == disposition {
                Ok(())
            } else {
                Err(invalid_token_error(
                    Some(self.queue.topic.as_str()),
                    "conflicting_disposition",
                ))
            };
        }
        let delivery = state
            .in_flight
            .get(token_state.token_id.as_ref())
            .ok_or_else(|| invalid_token_error(Some(self.queue.topic.as_str()), "unknown_token"))?;
        if !Arc::ptr_eq(&delivery.settlement, &settlement) {
            return Err(invalid_token_error(Some(self.queue.topic.as_str()), "unknown_token"));
        }
        let event = state
            .in_flight
            .remove(token_state.token_id.as_ref())
            .expect("in-flight event was validated above");
        if disposition == DeliveryDisposition::Retry {
            state.enqueue_front(event.event);
            self.queue.ready.notify_one();
        } else {
            self.shared.outstanding.release(1, event.event.weight_bytes);
        }
        token_state.disposition = Some(disposition);
        drop(token_state);
        drop(state);
        if disposition == DeliveryDisposition::Retry {
            self.queue.async_ready.notify_all();
        }
        signal_changed(&self.shared);
        Ok(())
    }

    /// Unregisters this receiver and discards queued and unsettled events.
    ///
    /// Releases their capacity reservations and wakes lifecycle waiters.
    /// Repeated calls preserve the closed state without releasing twice.
    ///
    /// # Returns
    /// `Ok(())`; this local close operation reports no SPI errors.
    fn close(&mut self) -> Result<(), SpiError> {
        {
            let mut state = self.queue.lock();
            if !state.closed {
                state.closed = true;
                let discarded = state.clear_pending();
                let weight = discarded.iter().map(|event| event.weight_bytes).sum();
                self.shared.outstanding.release(discarded.len(), weight);
                self.queue.ready.notify_all();
                drop(state);
                drop(discarded);
            }
        }
        self.queue.async_ready.notify_all();
        let mut state = self.shared.state.lock().unwrap_or_else(PoisonError::into_inner);
        let topic = self.queue.topic.clone();
        let removed = state
            .topics
            .get_mut(&topic)
            .is_some_and(|bucket| bucket.remove(self.queue.id, &self.queue));
        if removed {
            state.subscription_ids.remove(&self.queue.id);
        }
        if state.topics.get(&topic).is_some_and(|bucket| bucket.queues.is_empty()) {
            state.topics.remove(&topic);
        }
        drop(state);
        signal_changed(&self.shared);
        Ok(())
    }
}

impl Drop for LocalEventSubscription {
    /// Closes the receiver and releases any unsettled local deliveries.
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(all(test, not(loom)))]
mod tests {

    use std::any::TypeId;
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;
    use std::time::SystemTime;

    use qubit_id::Id;

    use crate::error::SpiError;
    use crate::local::LocalEventBusConfig;
    use crate::local::local_event_bus_spi::LocalEventBusSpi;
    use crate::model::EventId;
    use crate::model::ProviderOptions;
    use crate::model::StartPosition;
    use crate::model::SubscriberId;
    use crate::model::SubscriptionDurability;
    use crate::spi::EventBusSpi;
    use crate::spi::OutboundMessage;
    use crate::spi::SpiSubscriptionRequest;
    use crate::spi::TopicAddress;
    use crate::spi::TransportPayload;

    /// Token exhaustion retains the pending event and its reservation until
    /// close.
    #[test]
    fn test_token_exhaustion_preserves_pending_weight_until_close() {
        let weight = NonZeroUsize::new(5).expect("positive weight");
        let spi = LocalEventBusSpi::new(&LocalEventBusConfig::new().max_total_outstanding_weight_bytes(weight))
            .expect("valid config");
        let topic = TopicAddress::new("local.token.exhaustion").expect("valid topic");
        let request = SpiSubscriptionRequest::new(
            Id::new(1),
            topic.clone(),
            SubscriberId::new("exhausted").expect("valid subscriber"),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<u32>(),
        );
        let mut receiver = spi.subscribe(request).expect("subscription");
        let message = OutboundMessage::new(
            topic.clone(),
            EventId::new("retained").expect("valid ID"),
            SystemTime::UNIX_EPOCH,
            Default::default(),
            None,
            None,
            TransportPayload::Native(Arc::new(7_u32)),
        )
        .with_native_payload_weight_bytes(weight);
        let _ = spi.publish(message).expect("admitted event");
        let queue = spi.shared.state.lock().expect("bus lock").live_queues_for_topic(&topic)[0].clone();
        queue.lock().next_delivery_token = u64::MAX;
        for timeout in [Duration::ZERO, Duration::MAX] {
            let result = receiver.receive(timeout);
            let state = queue.lock();
            assert_eq!(state.pending_count(), 1, "failed receive must retain the event");
            assert!(state.in_flight.is_empty());
            assert_eq!(state.next_delivery_token, u64::MAX);
            drop(state);
            assert!(matches!(
                result,
                Err(SpiError::Operation {
                    operation: "receive",
                    kind: "settlement_token_exhausted",
                    retryable: Some(false),
                    ..
                })
            ));
        }
        assert!(!spi.shared.outstanding.try_acquire(1), "pending event keeps its weight");
        receiver.close().expect("close removes retained event");
        assert!(
            spi.shared.outstanding.try_acquire(weight.get()),
            "close returns the entire reservation"
        );
        spi.shared.outstanding.release(1, weight.get());
    }
}
