// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-owner synchronous receiver for a local subscription.

use std::sync::Arc;
use std::sync::PoisonError;
use std::time::Duration;
use std::time::Instant;

use super::local_event_bus_spi::invalid_token_error;
use super::local_event_bus_spi::operation_error;
use super::local_event_bus_spi::signal_changed;
use super::state::LocalQueue;
use super::state::LocalSettlementState;
use super::state::LocalSharedState;
use crate::error::SpiError;
use crate::spi::DeliveryDisposition;
use crate::spi::EventSubscriptionSpi;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;

/// Single-owner synchronous receiver for one local subscription.
pub(super) struct LocalEventSubscription {
    /// Shared bus state used for lifecycle notifications and unregistering.
    shared: Arc<LocalSharedState>,
    /// Per-subscription queue and settlement state.
    queue: Arc<LocalQueue>,
}

impl LocalEventSubscription {
    /// Creates a receiver bound to a registered local subscription queue.
    pub(super) fn new(shared: Arc<LocalSharedState>, queue: Arc<LocalQueue>) -> Self {
        Self { shared, queue }
    }
}

impl EventSubscriptionSpi for LocalEventSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let started = Instant::now();
        let mut state = self.queue.lock();
        loop {
            if state.closed {
                return Ok(ReceiveOutcome::Closed);
            }
            let now = Instant::now();
            if let Some(event) = state.pop_ready(now) {
                let sequence = state.next_delivery_token.checked_add(1).ok_or_else(|| {
                    operation_error("receive", Some(self.queue.topic.as_str()), "settlement_token_exhausted")
                })?;
                state.next_delivery_token = sequence;
                let token = format!("{}:{sequence}", event.event_id()).into_boxed_str();
                let settlement = Arc::new(std::sync::Mutex::new(LocalSettlementState {
                    token_id: token.clone(),
                    disposition: None,
                }));
                state.in_flight.insert(
                    token,
                    super::state::LocalInFlight {
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

    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        if !token.belongs_to(self.queue.id) {
            return Err(invalid_token_error(
                Some(self.queue.topic.as_str()),
                "foreign_subscription",
            ));
        }
        let settlement = token
            .downcast_ref::<super::state::LocalSettlementHandle>()
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

    fn close(&mut self) -> Result<(), SpiError> {
        {
            let mut state = self.queue.lock();
            if !state.closed {
                state.closed = true;
                state.clear_pending();
                state.in_flight.clear();
                self.queue.ready.notify_all();
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
    fn drop(&mut self) {
        let _ = self.close();
    }
}
